from __future__ import annotations

from typing import Any

import psycopg

from .changes import Changes
from .db import execute
from .schemainspect import DBInspector, get_inspector
from .statements import Statements


class Migration:
    """
    The main class of migra
    """

    def __init__(
        self,
        x_from: DBInspector | psycopg.Connection[Any] | None,
        x_target: DBInspector | psycopg.Connection[Any] | None,
        schema: str | list[str] | None = None,
        exclude_schema: str | None = None,
        ignore_extension_versions: bool = False,
    ) -> None:
        self.statements = Statements()
        self.changes = Changes(None, None)
        if schema and exclude_schema:
            raise ValueError("You cannot have both a schema and excluded schema")
        self.schema = schema
        self.exclude_schema = exclude_schema
        if isinstance(x_from, DBInspector):
            self.changes.i_from = x_from
        else:
            self.changes.i_from = get_inspector(
                x_from, schema=schema, exclude_schema=exclude_schema
            )
            if x_from:
                self.s_from = x_from
        if isinstance(x_target, DBInspector):
            self.changes.i_target = x_target
        else:
            self.changes.i_target = get_inspector(
                x_target, schema=schema, exclude_schema=exclude_schema
            )
            if x_target:
                self.s_target = x_target

        self.changes.ignore_extension_versions = ignore_extension_versions

    def inspect_from(self) -> None:
        self.changes.i_from = get_inspector(
            self.s_from, schema=self.schema, exclude_schema=self.exclude_schema
        )

    def inspect_target(self) -> None:
        self.changes.i_target = get_inspector(
            self.s_target, schema=self.schema, exclude_schema=self.exclude_schema
        )

    def clear(self) -> None:
        self.statements = Statements()

    def apply(self) -> None:
        for stmt in self.statements:
            execute(self.s_from, stmt)
        self.changes.i_from = get_inspector(
            self.s_from, schema=self.schema, exclude_schema=self.exclude_schema
        )
        safety_on = self.statements.safe
        self.clear()
        self.set_safety(safety_on)

    def add(self, statements: Statements) -> None:
        self.statements += statements

    def add_sql(self, sql: str) -> None:
        self.statements += Statements([sql])

    def set_safety(self, safety_on: bool) -> None:
        self.statements.safe = safety_on

    def add_extension_changes(self, creates: bool = True, drops: bool = True) -> None:
        if creates:
            self.add(self.changes.extensions(creations_only=True))
        if drops:
            self.add(self.changes.extensions(drops_only=True))

    def add_all_changes(
        self,
        privileges: bool = False,
        concurrent_indexes: bool = False,
        roles: bool = False,
    ) -> None:
        if roles:
            self.add(self.changes.roles(creations_only=True))

        self.add(self.changes.schemas(creations_only=True))

        self.add(self.changes.extensions(creations_only=True, modifications=False))
        self.add(self.changes.extensions(modifications_only=True, modifications=True))
        self.add(self.changes.collations(creations_only=True))
        self.add(self.changes.enums(creations_only=True, modifications=False))
        self.add(self.changes.domains(creations_only=True))
        self.add(self.changes.range_types(creations_only=True))
        self.add(self.changes.sequences(creations_only=True))
        self.add(self.changes.triggers(drops_only=True))
        self.add(self.changes.rlspolicies(drops_only=True, modifications=False))
        if privileges:
            self.add(self.changes.privileges(drops_only=True))
        self.add(self.changes.non_pk_constraints(drops_only=True))

        self.add(self.changes.comments(drops_only=True))
        self.add(self.changes.mv_indexes(drops_only=True))
        self.add(self.changes.non_table_selectable_drops())

        self.add(self.changes.pk_constraints(drops_only=True))
        self.add(self.changes.non_mv_indexes(drops_only=True))

        self.add(self.changes.tables_only_selectables())

        self.add(self.changes.sequences(drops_only=True))
        self.add(self.changes.range_types(drops_only=True))
        self.add(self.changes.domains(drops_only=True))
        self.add(self.changes.enums(drops_only=True, modifications=False))
        self.add(self.changes.extensions(drops_only=True, modifications=False))
        self.add(self.changes.non_mv_indexes(creations_only=True))
        self.add(self.changes.pk_constraints(creations_only=True))
        self.add(self.changes.non_pk_constraints(creations_only=True))

        self.add(self.changes.non_table_selectable_creations())
        self.add(self.changes.mv_indexes(creations_only=True))

        if privileges:
            self.add(self.changes.privileges(creations_only=True))
        self.add(self.changes.rlspolicies(modifications_only=True))
        self.add(self.changes.rlspolicies(creations_only=True, modifications=False))
        self.add(self.changes.triggers(creations_only=True))
        self.add(self.changes.comments(creations_only=True))
        self.add(self.changes.collations(drops_only=True))

        if roles:
            self.add(self.changes.roles(drops_only=True))

        self.add(self.changes.schemas(drops_only=True))

        if concurrent_indexes:
            new_stmts = Statements()
            for s in self.statements:
                lower = s.lstrip().lower()
                if lower.startswith("create index") and "concurrently" not in lower:
                    idx = s.lower().index("create index") + len("create index")
                    s = s[:idx] + " concurrently" + s[idx:]
                new_stmts.append(s)
            new_stmts.safe = self.statements.safe
            self.statements = new_stmts

    @property
    def sql(self) -> str:
        return self.statements.sql
