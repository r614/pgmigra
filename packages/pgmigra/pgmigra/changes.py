from __future__ import annotations

from functools import partial
from graphlib import TopologicalSorter
from typing import Any, TypeVar

from .schemainspect.inspected import Inspected
from .schemainspect.misc import quoted_identifier
from .schemainspect.pg.objects import (
    InspectedEnum,
    InspectedIndex,
    InspectedSelectable,
    InspectedSequence,
    InspectedTrigger,
)
from .schemainspect.pg.registry import REGISTRY
from .statements import Statements

PK = "PRIMARY KEY"

InspectedT = TypeVar("InspectedT", bound=Inspected)
V = TypeVar("V")


def differences(
    a: dict[str, V],
    b: dict[str, V],
) -> tuple[dict[str, V], dict[str, V], dict[str, V], dict[str, V]]:
    a_keys = set(a.keys())
    b_keys = set(b.keys())
    keys_added = b_keys - a_keys
    keys_removed = a_keys - b_keys
    keys_common = a_keys & b_keys
    added = {k: b[k] for k in sorted(keys_added)}
    removed = {k: a[k] for k in sorted(keys_removed)}
    modified = {k: b[k] for k in sorted(keys_common) if a[k] != b[k]}
    unmodified = {k: b[k] for k in sorted(keys_common) if a[k] == b[k]}
    return added, removed, modified, unmodified


def statements_for_changes(
    things_from: dict[str, InspectedT],
    things_target: dict[str, InspectedT],
    creations_only: bool = False,
    drops_only: bool = False,
    modifications_only: bool = False,
    modifications: bool = True,
    dependency_ordering: bool = False,
    add_dependents_for_modified: bool = False,
    modifications_as_alters: bool = False,
) -> Statements:
    added, removed, modified, unmodified = differences(things_from, things_target)

    return statements_from_differences(
        added=added,
        removed=removed,
        modified=modified,
        replaceable=None,
        creations_only=creations_only,
        drops_only=drops_only,
        modifications_only=modifications_only,
        modifications=modifications,
        dependency_ordering=dependency_ordering,
        old=things_from,
        modifications_as_alters=modifications_as_alters,
    )


def statements_from_differences(
    added: dict[str, InspectedT],
    removed: dict[str, InspectedT],
    modified: dict[str, InspectedT],
    replaceable: set[str] | None = None,
    creations_only: bool = False,
    drops_only: bool = False,
    modifications: bool = True,
    dependency_ordering: bool = False,
    old: dict[str, InspectedT] | None = None,
    modifications_only: bool = False,
    modifications_as_alters: bool = False,
) -> Statements:
    replaceable = replaceable or set()
    statements = Statements()

    pending_creations: set[str] = set()
    pending_drops: set[str] = set()

    creations = not (drops_only or modifications_only)
    drops = not (creations_only or modifications_only)
    modifications = (
        modifications or modifications_only and not (creations_only or drops_only)
    )

    drop_and_recreate = modifications and not modifications_as_alters
    alters = modifications and modifications_as_alters and not drops_only

    if drops:
        pending_drops |= set(removed)

    if creations:
        pending_creations |= set(added)

    if drop_and_recreate:
        if drops:
            pending_drops |= set(modified) - replaceable

        if creations:
            pending_creations |= set(modified)

    if alters:
        assert old is not None
        for k, v in modified.items():
            statements += v.alter_statements(old[k])

    def has_remaining_dependents(v: InspectedT, pending_drops: set[str]) -> bool:
        if not dependency_ordering:
            return False

        return bool(set(v.dependents) & pending_drops)

    def has_uncreated_dependencies(v: InspectedT, pending_creations: set[str]) -> bool:
        if not dependency_ordering:
            return False

        return bool(set(v.dependent_on) & pending_creations)

    while True:
        assert old is not None
        before = pending_drops | pending_creations
        if drops:
            for k, v in removed.items():
                if not has_remaining_dependents(v, pending_drops):
                    if k in pending_drops:
                        statements.append(old[k].drop_statement)
                        pending_drops.remove(k)
        if creations:
            for k, v in added.items():
                if not has_uncreated_dependencies(v, pending_creations):
                    if k in pending_creations:
                        safer: list[str] | None = getattr(
                            v, "safer_create_statements", None
                        )
                        if safer is not None:
                            statements += safer
                        else:
                            statements.append(v.create_statement)
                        pending_creations.remove(k)
        if modifications:
            for k, v in modified.items():
                if drops:
                    if not has_remaining_dependents(v, pending_drops):
                        if k in pending_drops:
                            statements.append(old[k].drop_statement)
                            pending_drops.remove(k)
                if creations:
                    if not has_uncreated_dependencies(v, pending_creations):
                        if k in pending_creations:
                            safer2: list[str] | None = getattr(
                                v, "safer_create_statements", None
                            )
                            if safer2 is not None:
                                statements += safer2
                            else:
                                statements.append(v.create_statement)
                            pending_creations.remove(k)
        after = pending_drops | pending_creations
        if not after:
            break

        elif (
            after == before
        ):  # this should never happen because there shouldn't be circular dependencies
            raise ValueError("cannot resolve dependencies")  # pragma: no cover

    return statements


def get_enum_modifications(
    tables_from: dict[str, InspectedSelectable],
    tables_target: dict[str, InspectedSelectable],
    enums_from: dict[str, InspectedEnum],
    enums_target: dict[str, InspectedEnum],
    return_tuple: bool = False,
) -> Statements | tuple[Statements, Statements]:
    _, _, e_modified, _ = differences(enums_from, enums_target)
    _, _, t_modified, _ = differences(tables_from, tables_target)
    pre = Statements()
    recreate = Statements()
    post = Statements()

    alterable: dict[str, InspectedEnum] = {}
    must_recreate: dict[str, InspectedEnum] = {}
    for k, v in e_modified.items():
        old = enums_from[k]
        if old.can_be_changed_to(v):
            alterable[k] = v
        else:
            must_recreate[k] = v

    for k, v in alterable.items():
        old = enums_from[k]
        for stmt in old.change_statements(v):
            pre.append(stmt)

    for t, v in t_modified.items():
        t_before = tables_from[t]
        _, _, c_modified, _ = differences(t_before.columns, v.columns)
        for k, c in c_modified.items():
            before = t_before.columns[k]

            if (
                (c.is_enum and before.is_enum)
                and c.dbtypestr == before.dbtypestr
                and c.enum != before.enum
                and before.enum.quoted_full_name not in alterable
            ):
                has_default = c.default and not c.is_generated

                if has_default:
                    pre.append(before.drop_default_statement(t))

                recast = c.change_enum_statement(v.quoted_full_name)

                recreate.append(recast)

                if has_default:
                    post.append(before.add_default_statement(t))

    unwanted_suffix = "__old_version_to_be_dropped"

    for e in must_recreate.values():
        unwanted_name = e.name + unwanted_suffix

        rename = e.alter_rename_statement(unwanted_name)
        pre.append(rename)

        pre.append(e.create_statement)

        drop_statement = e.drop_statement_with_rename(unwanted_name)

        post.append(drop_statement)

    if return_tuple:
        return pre, recreate + post
    else:
        return pre + recreate + post


def get_table_changes(
    tables_from: dict[str, InspectedSelectable],
    tables_target: dict[str, InspectedSelectable],
    enums_from: dict[str, InspectedEnum],
    enums_target: dict[str, InspectedEnum],
    sequences_from: dict[str, InspectedSequence],
    sequences_target: dict[str, InspectedSequence],
) -> Statements:
    added, removed, modified, _ = differences(tables_from, tables_target)

    statements = Statements()
    # Sort drops in reverse dependency order
    if removed:
        drop_graph: dict[str, set[str]] = {}
        for t, v in removed.items():
            deps_in_removed = {d for d in v.dependents if d in removed}
            drop_graph[t] = deps_in_removed
        try:
            drop_order = list(TopologicalSorter(drop_graph).static_order())
        except Exception:
            drop_order = list(removed.keys())
        for t in drop_order:
            statements.append(removed[t].drop_statement)

    enums_pre, enums_post = get_enum_modifications(
        tables_from, tables_target, enums_from, enums_target, return_tuple=True
    )

    statements += enums_pre

    # Sort creates in dependency order
    if added:
        create_graph: dict[str, set[str]] = {}
        for t, v in added.items():
            deps_in_added = {d for d in v.dependent_on if d in added}
            create_graph[t] = deps_in_added
        try:
            create_order = list(TopologicalSorter(create_graph).static_order())
        except Exception:
            create_order = list(added.keys())
        for t in create_order:
            v = added[t]
            statements.append(v.create_statement)
            if v.rowsecurity:
                statements.append(v.alter_rls_statement)
            if v.forcerowsecurity:
                statements.append(v.alter_force_rls_statement)

    statements += enums_post

    for t, v in modified.items():
        before = tables_from[t]

        # drop/recreate tables which have changed from partitioned to non-partitioned
        if v.is_partitioned != before.is_partitioned:
            statements.append(v.drop_statement)
            statements.append(v.create_statement)
            continue

        if v.is_unlogged != before.is_unlogged:
            statements += [v.alter_unlogged_statement]

        if v.owner and before.owner and v.owner != before.owner:
            statements.append(
                f"alter table {v.quoted_full_name} owner to {quoted_identifier(v.owner)};"
            )

        # attach/detach tables with changed parent tables
        if v.parent_table != before.parent_table:
            statements += v.attach_detach_statements(before)

    modified_order = list(modified.keys())

    modified_order.sort(key=lambda x: modified[x].is_inheritance_child_table)

    for t in modified_order:
        v = modified[t]

        before = tables_from[t]

        if not v.is_alterable:
            continue

        c_added, c_removed, c_modified, _ = differences(before.columns, v.columns)

        for k in list(c_modified):
            c = v.columns[k]
            c_before = before.columns[k]

            # there's no way to alter a table into/out of generated state
            # so you gotta drop/recreate

            generated_status_changed = c.is_generated != c_before.is_generated

            inheritance_status_changed = c.is_inherited != c_before.is_inherited

            generated_status_removed = not c.is_generated and c_before.is_generated

            drop_and_recreate_required = inheritance_status_changed or (
                generated_status_changed and not generated_status_removed
            )

            if drop_and_recreate_required:
                del c_modified[k]

                if not c_before.is_inherited:
                    c_removed[k] = c_before

                if not c.is_inherited:
                    c_added[k] = c

            if generated_status_changed:
                pass

        for k, c in c_removed.items():
            alter = v.alter_table_statement(c.drop_column_clause)
            statements.append(alter)
        for k, c in c_added.items():
            alter = v.alter_table_statement(c.add_column_clause)
            statements.append(alter)
        for k, c in c_modified.items():
            c_before = before.columns[k]
            statements += c.alter_table_statements(c_before, t)

        if v.rowsecurity != before.rowsecurity:
            statements.append(v.alter_rls_statement)

        if v.forcerowsecurity != before.forcerowsecurity:
            statements.append(v.alter_force_rls_statement)

    seq_created, seq_dropped, seq_modified, _ = differences(
        sequences_from, sequences_target
    )

    for k in seq_created:
        seq_b = sequences_target[k]

        if seq_b.quoted_table_and_column_name:
            statements.append(seq_b.alter_ownership_statement)

    for k in seq_modified:
        seq_a = sequences_from[k]
        seq_b = sequences_target[k]

        if seq_a.quoted_table_and_column_name != seq_b.quoted_table_and_column_name:
            statements.append(seq_b.alter_ownership_statement)

    return statements


def get_selectable_differences(
    selectables_from: dict[str, InspectedSelectable],
    selectables_target: dict[str, InspectedSelectable],
    enums_from: dict[str, InspectedEnum],
    enums_target: dict[str, InspectedEnum],
    add_dependents_for_modified: bool = True,
) -> tuple[
    dict[str, InspectedSelectable],
    dict[str, InspectedSelectable],
    dict[str, InspectedSelectable],
    dict[str, InspectedSelectable],
    dict[str, InspectedSelectable],
    dict[str, InspectedSelectable],
    dict[str, InspectedSelectable],
    dict[str, InspectedSelectable],
    set[str],
]:
    tables_from = {k: v for k, v in selectables_from.items() if v.is_table}
    tables_target = {k: v for k, v in selectables_target.items() if v.is_table}

    other_from = {k: v for k, v in selectables_from.items() if not v.is_table}
    other_target = {k: v for k, v in selectables_target.items() if not v.is_table}

    added_tables, removed_tables, modified_tables, unmodified_tables = differences(
        tables_from, tables_target
    )
    added_other, removed_other, modified_other, unmodified_other = differences(
        other_from, other_target
    )

    _, _, modified_enums, _ = differences(enums_from, enums_target)

    changed_all: dict[str, InspectedSelectable] = {}
    changed_all.update(modified_tables)
    changed_all.update(modified_other)
    modified_all = dict(changed_all)
    changed_all.update(removed_tables)
    changed_all.update(removed_other)

    replaceable: set[str] = set()
    not_replaceable: set[str] = set()

    if add_dependents_for_modified:
        for k, m in changed_all.items():
            old = selectables_from[k]

            if k in modified_all and m.can_replace(old):
                if not m.is_table:
                    changed_enums = [_ for _ in m.dependent_on if _ in modified_enums]
                    if not changed_enums:
                        replaceable.add(k)

                continue

            for d in m.dependents_all:
                if d in unmodified_other:
                    dd = unmodified_other.pop(d)
                    modified_other[d] = dd
                not_replaceable.add(d)
        modified_other = dict(sorted(modified_other.items()))

    replaceable -= not_replaceable

    return (
        tables_from,
        tables_target,
        added_tables,
        removed_tables,
        modified_tables,
        added_other,
        removed_other,
        modified_other,
        replaceable,
    )


def get_trigger_changes(
    triggers_from: dict[str, InspectedTrigger],
    triggers_target: dict[str, InspectedTrigger],
    selectables_from: dict[str, InspectedSelectable],
    selectables_target: dict[str, InspectedSelectable],
    enums_from: dict[str, InspectedEnum],
    enums_target: dict[str, InspectedEnum],
    add_dependents_for_modified: bool = True,
    **kwargs: Any,
) -> Statements:
    (
        _,
        _,
        _,
        _,
        modified_tables,
        _,
        _,
        modified_other,
        replaceable,
    ) = get_selectable_differences(
        selectables_from,
        selectables_target,
        enums_from,
        enums_target,
        add_dependents_for_modified,
    )

    added, removed, modified, unmodified = differences(triggers_from, triggers_target)

    modified_tables_and_other = set(modified_other)
    deps_modified = [
        k
        for k, v in unmodified.items()
        if v.quoted_full_selectable_name in modified_tables_and_other
        and v.quoted_full_selectable_name not in replaceable
    ]

    for k in deps_modified:
        modified[k] = unmodified.pop(k)

    return statements_from_differences(
        added, removed, modified, old=triggers_from, **kwargs
    )


def get_selectable_changes(
    selectables_from: dict[str, InspectedSelectable],
    selectables_target: dict[str, InspectedSelectable],
    enums_from: dict[str, InspectedEnum],
    enums_target: dict[str, InspectedEnum],
    sequences_from: dict[str, InspectedSequence],
    sequences_target: dict[str, InspectedSequence],
    add_dependents_for_modified: bool = True,
    tables_only: bool = False,
    non_tables_only: bool = False,
    drops_only: bool = False,
    creations_only: bool = False,
) -> Statements:
    (
        tables_from,
        tables_target,
        _,
        _,
        _,
        added_other,
        removed_other,
        modified_other,
        replaceable,
    ) = get_selectable_differences(
        selectables_from,
        selectables_target,
        enums_from,
        enums_target,
        add_dependents_for_modified,
    )
    statements = Statements()

    def functions(d: dict[str, InspectedSelectable]) -> dict[str, InspectedSelectable]:
        return {k: v for k, v in d.items() if v.relationtype in ("f", "a")}

    if not tables_only:
        if not creations_only:
            statements += statements_from_differences(
                added_other,
                removed_other,
                modified_other,
                replaceable=replaceable,
                drops_only=True,
                dependency_ordering=True,
                old=selectables_from,
            )

    if not non_tables_only:
        statements += get_table_changes(
            tables_from,
            tables_target,
            enums_from,
            enums_target,
            sequences_from,
            sequences_target,
        )

    if not tables_only:
        if not drops_only:
            if any([functions(added_other), functions(modified_other)]):
                statements += ["set check_function_bodies = off;"]

            statements += statements_from_differences(
                added_other,
                removed_other,
                modified_other,
                replaceable=replaceable,
                creations_only=True,
                dependency_ordering=True,
                old=selectables_from,
            )

            for k in list(added_other) + list(modified_other):
                if k in selectables_target:
                    new = selectables_target[k]
                    if k in selectables_from:
                        old = selectables_from[k]
                        if (
                            hasattr(new, "owner")
                            and hasattr(old, "owner")
                            and new.owner
                            and old.owner
                            and new.owner != old.owner
                        ):
                            if new.relationtype == "v":
                                statements.append(
                                    f"alter view {new.quoted_full_name} owner to {quoted_identifier(new.owner)};"
                                )
                            elif new.relationtype == "m":
                                statements.append(
                                    f"alter materialized view {new.quoted_full_name} owner to {quoted_identifier(new.owner)};"
                                )
    return statements


class Changes:
    def __init__(
        self,
        i_from: Any,
        i_target: Any,
        ignore_extension_versions: bool = False,
    ) -> None:
        self.i_from = i_from
        self.i_target = i_target
        self.ignore_extension_versions = ignore_extension_versions

    @property
    def extensions(self) -> partial[Statements]:
        if self.ignore_extension_versions:
            fe = self.i_from.extensions_without_versions
            te = self.i_target.extensions_without_versions

            return partial(statements_for_changes, fe, te, modifications=False)
        else:
            return partial(
                statements_for_changes,
                self.i_from.extensions,
                self.i_target.extensions,
                modifications_as_alters=True,
            )

    @property
    def selectables(self) -> partial[Statements]:
        return partial(
            get_selectable_changes,
            dict(sorted(self.i_from.selectables.items())),
            dict(sorted(self.i_target.selectables.items())),
            self.i_from.enums,
            self.i_target.enums,
            self.i_from.sequences,
            self.i_target.sequences,
        )

    @property
    def tables_only_selectables(self) -> partial[Statements]:
        return partial(
            get_selectable_changes,
            dict(sorted(self.i_from.selectables.items())),
            dict(sorted(self.i_target.selectables.items())),
            self.i_from.enums,
            self.i_target.enums,
            self.i_from.sequences,
            self.i_target.sequences,
            tables_only=True,
        )

    @property
    def non_table_selectable_drops(self) -> partial[Statements]:
        return partial(
            get_selectable_changes,
            dict(sorted(self.i_from.selectables.items())),
            dict(sorted(self.i_target.selectables.items())),
            self.i_from.enums,
            self.i_target.enums,
            self.i_from.sequences,
            self.i_target.sequences,
            drops_only=True,
            non_tables_only=True,
        )

    @property
    def non_table_selectable_creations(self) -> partial[Statements]:
        return partial(
            get_selectable_changes,
            dict(sorted(self.i_from.selectables.items())),
            dict(sorted(self.i_target.selectables.items())),
            self.i_from.enums,
            self.i_target.enums,
            self.i_from.sequences,
            self.i_target.sequences,
            creations_only=True,
            non_tables_only=True,
        )

    @property
    def non_pk_constraints(self) -> partial[Statements]:
        a = self.i_from.constraints.items()
        b = self.i_target.constraints.items()
        a_od = {k: v for k, v in a if v.constraint_type != PK}
        b_od = {k: v for k, v in b if v.constraint_type != PK}
        return partial(statements_for_changes, a_od, b_od)

    @property
    def pk_constraints(self) -> partial[Statements]:
        a = self.i_from.constraints.items()
        b = self.i_target.constraints.items()
        a_od = {k: v for k, v in a if v.constraint_type == PK}
        b_od = {k: v for k, v in b if v.constraint_type == PK}
        return partial(statements_for_changes, a_od, b_od)

    @property
    def triggers(self) -> partial[Statements]:
        return partial(
            get_trigger_changes,
            dict(sorted(self.i_from.triggers.items())),
            dict(sorted(self.i_target.triggers.items())),
            dict(sorted(self.i_from.selectables.items())),
            dict(sorted(self.i_target.selectables.items())),
            self.i_from.enums,
            self.i_target.enums,
        )

    @staticmethod
    def _is_mv_index(i: InspectedIndex, ii: Any) -> bool:
        sig = quoted_identifier(i.table_name, i.schema)
        return sig in ii.materialized_views

    def _modified_matview_sigs(self) -> set[str]:
        from_mvs = self.i_from.materialized_views
        target_mvs = self.i_target.materialized_views
        return {k for k in from_mvs if k in target_mvs and from_mvs[k] != target_mvs[k]}

    @property
    def mv_indexes(self) -> partial[Statements]:
        a = self.i_from.indexes.items()
        b = self.i_target.indexes.items()
        a_od = {k: v for k, v in a if self._is_mv_index(v, self.i_from)}
        b_od = {k: v for k, v in b if self._is_mv_index(v, self.i_target)}
        # When a matview is modified it gets drop+recreated, which implicitly
        # drops its indexes.  Remove those from a_od so they appear as "added"
        # in the target and get CREATE statements in the creations phase.
        modified_mvs = self._modified_matview_sigs()
        if modified_mvs:
            a_od = {
                k: v
                for k, v in a_od.items()
                if quoted_identifier(v.table_name, v.schema) not in modified_mvs
            }
        return partial(statements_for_changes, a_od, b_od)

    @property
    def non_mv_indexes(self) -> partial[Statements]:
        a = self.i_from.indexes.items()
        b = self.i_target.indexes.items()
        a_od = {k: v for k, v in a if not self._is_mv_index(v, self.i_from)}
        b_od = {k: v for k, v in b if not self._is_mv_index(v, self.i_target)}
        return partial(statements_for_changes, a_od, b_od)

    def __getattr__(self, name: str) -> partial[Statements]:
        if name in REGISTRY:
            obj_type = REGISTRY[name]
            kwargs: dict[str, Any] = {}
            if obj_type.modification == "alter":
                kwargs["modifications_as_alters"] = True
            elif obj_type.modification == "none":
                kwargs["modifications"] = False
            return partial(
                statements_for_changes,
                getattr(self.i_from, name),
                getattr(self.i_target, name),
                **kwargs,
            )
        raise AttributeError(name)
