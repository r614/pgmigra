from __future__ import annotations

from dataclasses import dataclass, field
from graphlib import TopologicalSorter
from typing import Any, Literal


@dataclass
class ObjectType:
    """Descriptor for a PostgreSQL object type."""

    name: str
    depends_on: tuple[str, ...] = ()
    modification: Literal["recreate", "alter", "none"] = "recreate"
    condition: Literal["privileges", "roles"] | None = None
    schema_filterable: bool = True
    include_in_eq: bool = True
    min_pg_version: int = 14


@dataclass
class DiffStep:
    """A single step in the DDL diff ordering."""

    name: str
    kwargs: dict[str, Any] = field(default_factory=dict)
    condition: Literal["privileges", "roles"] | None = None


DIFF_ORDER: list[ObjectType | DiffStep] = [
    # Infrastructure types (created before core, dropped after)
    ObjectType("schemas"),
    ObjectType("extensions", modification="alter"),
    ObjectType("enums", modification="none"),
    ObjectType("domains"),
    ObjectType("range_types"),
    ObjectType("sequences", modification="none"),
    ObjectType("collations"),
    ObjectType("fdws", schema_filterable=False),
    ObjectType("foreign_servers", depends_on=("fdws",), schema_filterable=False),
    ObjectType(
        "user_mappings", depends_on=("foreign_servers",), schema_filterable=False
    ),
    ObjectType("ts_dicts"),
    ObjectType("ts_configs", depends_on=("ts_dicts",)),
    ObjectType("operator_families"),
    # Core sandwich — fixed order for table/selectable/type changes
    DiffStep("non_pk_constraints", {"drops_only": True}),
    DiffStep("mv_indexes", {"drops_only": True}),
    DiffStep("non_table_selectable_drops"),
    DiffStep("pk_constraints", {"drops_only": True}),
    DiffStep("non_mv_indexes", {"drops_only": True}),
    DiffStep("tables_only_selectables"),
    DiffStep("sequences", {"drops_only": True}),
    DiffStep("range_types", {"drops_only": True}),
    DiffStep("domains", {"drops_only": True}),
    DiffStep("enums", {"drops_only": True, "modifications": False}),
    DiffStep("extensions", {"drops_only": True, "modifications": False}),
    DiffStep("non_mv_indexes", {"creations_only": True}),
    DiffStep("pk_constraints", {"creations_only": True}),
    DiffStep("non_pk_constraints", {"creations_only": True}),
    DiffStep("non_table_selectable_creations"),
    DiffStep("mv_indexes", {"creations_only": True}),
    # Types depending on selectables (dropped before core, created after)
    ObjectType("operators", depends_on=("selectables",)),
    ObjectType(
        "operator_classes", depends_on=("selectables", "operators", "operator_families")
    ),
    ObjectType("triggers", depends_on=("selectables",)),
    ObjectType("rules", depends_on=("selectables",)),
    ObjectType("statistics", depends_on=("selectables",)),
    ObjectType("rlspolicies", depends_on=("selectables",)),
    ObjectType(
        "publications",
        depends_on=("selectables",),
        modification="alter",
        schema_filterable=False,
    ),
    ObjectType("event_triggers", depends_on=("selectables",), schema_filterable=False),
    ObjectType("casts", depends_on=("selectables",), schema_filterable=False),
    ObjectType("comments", depends_on=("selectables",)),
    ObjectType(
        "privileges",
        depends_on=("selectables",),
        condition="privileges",
        include_in_eq=False,
    ),
    ObjectType(
        "roles",
        modification="alter",
        condition="roles",
        schema_filterable=False,
        include_in_eq=False,
    ),
]

_object_types = [e for e in DIFF_ORDER if isinstance(e, ObjectType)]
REGISTRY: dict[str, ObjectType] = {ot.name: ot for ot in _object_types}
REGISTRY["constraints"] = ObjectType("constraints")
REGISTRY["indexes"] = ObjectType("indexes", include_in_eq=False)

COMPOUND_PROPS = [
    "relations",
    "tables",
    "views",
    "functions",
    "selectables",
    "foreign_tables",
]


def _depends_on_selectables(name: str) -> bool:
    """Check if a type transitively depends on 'selectables'."""
    visited: set[str] = set()
    stack = [name]
    while stack:
        current = stack.pop()
        if current in visited:
            continue
        visited.add(current)
        if current == "selectables":
            return True
        if current in REGISTRY:
            stack.extend(REGISTRY[current].depends_on)
    return False


def _topo_sort_steps(steps: list[DiffStep], reverse: bool = False) -> list[DiffStep]:
    """Topologically sort steps based on ObjectType.depends_on."""
    names_seen: list[str] = []
    name_to_steps: dict[str, list[DiffStep]] = {}
    for step in steps:
        if step.name not in name_to_steps:
            names_seen.append(step.name)
            name_to_steps[step.name] = []
        name_to_steps[step.name].append(step)

    names_in_phase = set(names_seen)

    graph: dict[str, set[str]] = {}
    for name in names_seen:
        if name in REGISTRY:
            deps = {d for d in REGISTRY[name].depends_on if d in names_in_phase}
        else:
            deps = set()
        graph[name] = deps

    sorted_names = list(TopologicalSorter(graph).static_order())

    if reverse:
        sorted_names = list(reversed(sorted_names))

    result: list[DiffStep] = []
    for name in sorted_names:
        result.extend(name_to_steps[name])
    return result


def build_diff_steps() -> list[DiffStep]:
    """Compute correctly-ordered DIFF_STEPS from DIFF_ORDER."""
    core_steps = [e for e in DIFF_ORDER if isinstance(e, DiffStep)]
    core_drop_names = {s.name for s in core_steps if s.kwargs.get("drops_only")}

    all_creates: list[DiffStep] = []
    all_drops: list[DiffStep] = []
    for ot in _object_types:
        all_creates.append(
            DiffStep(ot.name, {"creations_only": True}, condition=ot.condition)
        )
        if ot.name not in core_drop_names:
            all_drops.append(
                DiffStep(ot.name, {"drops_only": True}, condition=ot.condition)
            )

    phase1 = [s for s in all_creates if not _depends_on_selectables(s.name)]
    phase4 = [s for s in all_creates if _depends_on_selectables(s.name)]
    phase2 = [s for s in all_drops if _depends_on_selectables(s.name)]
    phase5 = [s for s in all_drops if not _depends_on_selectables(s.name)]

    return (
        _topo_sort_steps(phase1)
        + _topo_sort_steps(phase2, reverse=True)
        + core_steps
        + _topo_sort_steps(phase4)
        + _topo_sort_steps(phase5, reverse=True)
    )


_diff_steps_cache: list[DiffStep] | None = None


def get_diff_steps() -> list[DiffStep]:
    """Return DIFF_STEPS, computing lazily."""
    global _diff_steps_cache  # noqa: PLW0603
    if _diff_steps_cache is None:
        _diff_steps_cache = build_diff_steps()
    return _diff_steps_cache
