"""Verify that build_diff_steps() produces a correctly ordered step list."""

from pgmigra.schemainspect.pg.registry import (
    DIFF_ORDER,
    REGISTRY,
    DiffStep,
    _depends_on_selectables,
    build_diff_steps,
    get_diff_steps,
)

_CORE_STEPS = [e for e in DIFF_ORDER if isinstance(e, DiffStep)]


def _find_step_index(steps, name, kwargs_match=None):
    """Find the index of the first step matching name and optional kwargs."""
    for i, s in enumerate(steps):
        if s.name == name:
            if kwargs_match is None or all(
                s.kwargs.get(k) == v for k, v in kwargs_match.items()
            ):
                return i
    return None


def test_diff_steps_nonempty():
    steps = get_diff_steps()
    assert len(steps) > 0


def test_lazy_caching():
    a = get_diff_steps()
    b = get_diff_steps()
    assert a is b


def test_all_registry_types_have_steps():
    """Every type in DIFF_ORDER must have a create step."""
    steps = build_diff_steps()
    step_names_create = {s.name for s in steps if s.kwargs.get("creations_only")}

    from pgmigra.schemainspect.pg.registry import _object_types

    for ot in _object_types:
        assert ot.name in step_names_create, f"{ot.name} has no create step"


def test_core_sandwich_position():
    """Core sandwich must appear as a contiguous block."""
    steps = build_diff_steps()
    core_start = None
    for i, s in enumerate(steps):
        if s is _CORE_STEPS[0]:
            core_start = i
            break
    assert core_start is not None, "Core sandwich not found"
    for j, core_step in enumerate(_CORE_STEPS):
        assert steps[core_start + j] is core_step, (
            f"Core sandwich broken at position {j}: expected {core_step.name}, "
            f"got {steps[core_start + j].name}"
        )


def test_dependent_drops_before_core():
    """Types depending on selectables must have their drops before the core sandwich."""
    steps = build_diff_steps()
    core_start = next(i for i, s in enumerate(steps) if s is _CORE_STEPS[0])

    for i, s in enumerate(steps):
        if s.kwargs.get("drops_only") and _depends_on_selectables(s.name):
            if s not in _CORE_STEPS:
                assert i < core_start, (
                    f"Drop of {s.name} (index {i}) should be before core (index {core_start})"
                )


def test_dependent_creates_after_core():
    """Types depending on selectables must have their creates after the core sandwich."""
    steps = build_diff_steps()
    core_end = next(i for i, s in enumerate(steps) if s is _CORE_STEPS[-1])

    for i, s in enumerate(steps):
        if s.kwargs.get("creations_only") and _depends_on_selectables(s.name):
            if s not in _CORE_STEPS:
                assert i > core_end, (
                    f"Create of {s.name} (index {i}) should be after core end (index {core_end})"
                )


def test_infra_creates_before_core():
    """Infrastructure types (not depending on selectables) must have creates before core."""
    steps = build_diff_steps()
    core_start = next(i for i, s in enumerate(steps) if s is _CORE_STEPS[0])

    for i, s in enumerate(steps):
        if s.kwargs.get("creations_only") and not _depends_on_selectables(s.name):
            if s not in _CORE_STEPS:
                assert i < core_start, (
                    f"Infra create of {s.name} (index {i}) should be before core (index {core_start})"
                )


def test_infra_drops_after_core():
    """Infrastructure types (not depending on selectables) must have drops after core."""
    steps = build_diff_steps()
    core_end = next(i for i, s in enumerate(steps) if s is _CORE_STEPS[-1])

    for i, s in enumerate(steps):
        if s.kwargs.get("drops_only") and not _depends_on_selectables(s.name):
            if s not in _CORE_STEPS:
                assert i > core_end, (
                    f"Infra drop of {s.name} (index {i}) should be after core end (index {core_end})"
                )


def test_dependency_order_in_creates():
    """Within create steps, dependencies must be created before dependents."""
    steps = build_diff_steps()
    create_indices = {}
    for i, s in enumerate(steps):
        if s.kwargs.get("creations_only") and s.name not in create_indices:
            create_indices[s.name] = i

    for name, obj_type in REGISTRY.items():
        if name not in create_indices:
            continue
        for dep in obj_type.depends_on:
            if dep in create_indices:
                assert create_indices[dep] < create_indices[name], (
                    f"Create: {dep} (index {create_indices[dep]}) must come before "
                    f"{name} (index {create_indices[name]})"
                )


def test_dependency_order_in_drops():
    """Within drop steps, dependents must be dropped before their dependencies."""
    steps = build_diff_steps()
    drop_indices = {}
    for i, s in enumerate(steps):
        if s.kwargs.get("drops_only") and s.name not in drop_indices:
            drop_indices[s.name] = i

    for name, obj_type in REGISTRY.items():
        if name not in drop_indices:
            continue
        for dep in obj_type.depends_on:
            if dep in drop_indices:
                assert drop_indices[name] < drop_indices[dep], (
                    f"Drop: {name} (index {drop_indices[name]}) must come before "
                    f"{dep} (index {drop_indices[dep]})"
                )


def test_specific_ordering_constraints():
    """Verify key ordering constraints that motivated this refactor."""
    steps = build_diff_steps()

    def idx(name, action=None):
        if action is None:
            return _find_step_index(steps, name)
        match = {"drops_only": True} if action == "drop" else {"creations_only": True}
        result = _find_step_index(steps, name, match)
        assert result is not None, f"No {action} step for {name}"
        return result

    # Event triggers depend on functions (selectables) — must drop before, create after
    assert idx("event_triggers", "drop") < idx("non_table_selectable_drops")
    assert idx("event_triggers", "create") > idx("non_table_selectable_creations")

    # Publications depend on tables — must drop before table changes, create after
    assert idx("publications", "drop") < idx("tables_only_selectables")
    assert idx("publications", "create") > idx("non_table_selectable_creations")

    # Operator classes depend on operators — create operators first, drop opclasses first
    assert idx("operators", "create") < idx("operator_classes", "create")
    assert idx("operator_classes", "drop") < idx("operators", "drop")

    # Foreign servers depend on fdws — create fdws first, drop foreign_servers first
    assert idx("fdws", "create") < idx("foreign_servers", "create")
    assert idx("foreign_servers", "drop") < idx("fdws", "drop")

    # User mappings depend on foreign_servers
    assert idx("foreign_servers", "create") < idx("user_mappings", "create")
    assert idx("user_mappings", "drop") < idx("foreign_servers", "drop")

    # TS configs depend on ts_dicts
    assert idx("ts_dicts", "create") < idx("ts_configs", "create")
    assert idx("ts_configs", "drop") < idx("ts_dicts", "drop")

    # RLS policies must drop before table column changes (core sandwich)
    assert idx("rlspolicies", "drop") < idx("tables_only_selectables")


def test_conditions_derived_from_registry():
    """Verify that step conditions match their ObjectType declarations."""
    steps = build_diff_steps()
    for s in steps:
        if s.name in REGISTRY:
            assert s.condition == REGISTRY[s.name].condition, (
                f"Step {s.name} has condition={s.condition} but registry says "
                f"condition={REGISTRY[s.name].condition}"
            )


def test_steps_derived_from_registry():
    """Verify that all peripheral steps come from ObjectType declarations."""
    steps = build_diff_steps()
    core_step_set = set(id(s) for s in _CORE_STEPS)
    for s in steps:
        if id(s) in core_step_set:
            continue
        assert s.name in REGISTRY, f"Peripheral step {s.name} not found in REGISTRY"
