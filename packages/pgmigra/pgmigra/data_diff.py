"""Table data diffing: generates INSERT/UPDATE/DELETE for annotated tables.

Tables annotated with ``pgmigra:data-diff`` in their PostgreSQL COMMENT are
eligible for row-level diffing.  The annotation supports structured options:

    COMMENT ON TABLE public.countries IS 'pgmigra:data-diff';
    COMMENT ON TABLE public.countries IS 'pgmigra:data-diff(ignore=updated_at,created_at)';
    COMMENT ON TABLE public.countries IS 'Some description. pgmigra:data-diff(ignore=updated_at)';

Options
-------
ignore : comma-separated column names to exclude from comparison (but still
         included in INSERT statements).
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from graphlib import TopologicalSorter
from typing import Any

from .schemainspect.misc import quoted_identifier
from .statements import Statements

# ---------------------------------------------------------------------------
# Annotation parsing
# ---------------------------------------------------------------------------

_ANNOTATION_RE = re.compile(
    r"pgmigra:data-diff(?:\(([^)]*)\))?",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class DataDiffAnnotation:
    """Parsed ``pgmigra:data-diff(...)`` annotation."""

    ignore: frozenset[str] = field(default_factory=frozenset)


def parse_data_diff_annotation(comment: str | None) -> DataDiffAnnotation | None:
    """Return a :class:`DataDiffAnnotation` if *comment* contains the marker, else ``None``."""
    if not comment:
        return None
    m = _ANNOTATION_RE.search(comment)
    if not m:
        return None
    opts_str = m.group(1)  # may be None when no parens
    ignore: frozenset[str] = frozenset()
    if opts_str:
        for part in opts_str.split(";"):
            part = part.strip()
            if not part:
                continue
            if "=" not in part:
                continue
            key, val = part.split("=", 1)
            key = key.strip().lower()
            if key == "ignore":
                ignore = frozenset(v.strip() for v in val.split(",") if v.strip())
    return DataDiffAnnotation(ignore=ignore)


# ---------------------------------------------------------------------------
# Row fetching
# ---------------------------------------------------------------------------

MAX_DATA_DIFF_ROWS = 50_000


def _pk_columns_for_table(
    table_name: str,
    schema: str,
    inspector: Any,
) -> list[str] | None:
    """Return the PK column names for a table, or ``None`` if no PK exists."""
    quoted_table = quoted_identifier(table_name, schema=schema)
    for c in inspector.constraints.values():
        if c.constraint_type == "PRIMARY KEY" and c.quoted_full_table_name == quoted_table:
            # PK columns are stored in the index's key_columns (as a list)
            if c.index and hasattr(c.index, "key_columns"):
                cols = c.index.key_columns
                if isinstance(cols, list):
                    return [col.strip('"') for col in cols]
                return [col.strip('"') for col in cols.split(", ")]
            # Fallback: parse from definition
            defn = c.definition  # e.g. "PRIMARY KEY (id)"
            inner = defn.split("(", 1)[1].rsplit(")", 1)[0]
            return [col.strip().strip('"') for col in inner.split(",")]
    return None


def _fetch_rows(
    conn: Any,
    schema: str,
    table_name: str,
    columns: list[str],
    pk_columns: list[str],
) -> list[dict[str, Any]]:
    """Fetch all rows from a table, ordered by PK."""
    col_list = ", ".join(quoted_identifier(c) for c in columns)
    order_by = ", ".join(quoted_identifier(c) for c in pk_columns)
    table = quoted_identifier(table_name, schema=schema)
    query = f"SELECT {col_list} FROM {table} ORDER BY {order_by}"  # noqa: S608
    from psycopg.rows import dict_row

    cur = conn.execute(query)
    # psycopg3 with namedtuple_row by default; convert to dicts
    rows = cur.fetchall()
    if rows and not isinstance(rows[0], dict):
        col_names = [desc[0] for desc in cur.description]
        return [dict(zip(col_names, row)) for row in rows]
    return rows


# ---------------------------------------------------------------------------
# SQL literal formatting
# ---------------------------------------------------------------------------


def _sql_literal(value: Any) -> str:
    """Format a Python value as a safe SQL literal."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return repr(value)
    if isinstance(value, bytes):
        return f"'\\x{value.hex()}'::bytea"
    if isinstance(value, (list, tuple)):
        inner = ", ".join(_sql_literal(v) for v in value)
        return f"ARRAY[{inner}]"
    # Default: treat as string
    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"


# ---------------------------------------------------------------------------
# Diff logic
# ---------------------------------------------------------------------------


@dataclass
class TableDataDiff:
    """Computed data diff for a single table."""

    schema: str
    table_name: str
    pk_columns: list[str]
    all_columns: list[str]
    non_pk_columns: list[str]
    inserts: list[dict[str, Any]]
    updates: list[dict[str, Any]]
    deletes: list[dict[str, Any]]

    @property
    def quoted_table(self) -> str:
        return quoted_identifier(self.table_name, schema=self.schema)

    def _upsert_statement(self, row: dict[str, Any]) -> str:
        """Generate INSERT ... ON CONFLICT (pk) DO UPDATE for a row."""
        cols = ", ".join(quoted_identifier(c) for c in self.all_columns)
        vals = ", ".join(_sql_literal(row[c]) for c in self.all_columns)
        pk_cols = ", ".join(quoted_identifier(c) for c in self.pk_columns)

        stmt = f"INSERT INTO {self.quoted_table} ({cols}) VALUES ({vals})"
        if self.non_pk_columns:
            updates = ", ".join(
                f"{quoted_identifier(c)} = EXCLUDED.{quoted_identifier(c)}"
                for c in self.non_pk_columns
            )
            stmt += f" ON CONFLICT ({pk_cols}) DO UPDATE SET {updates}"
        else:
            stmt += f" ON CONFLICT ({pk_cols}) DO NOTHING"
        return stmt + ";"

    def _delete_statement(self, row: dict[str, Any]) -> str:
        """Generate DELETE for a row identified by PK."""
        conditions = []
        for c in self.pk_columns:
            v = row[c]
            if v is None:
                conditions.append(f"{quoted_identifier(c)} IS NULL")
            else:
                conditions.append(f"{quoted_identifier(c)} = {_sql_literal(v)}")
        where = " AND ".join(conditions)
        return f"DELETE FROM {self.quoted_table} WHERE {where};"

    def statements(self, safe: bool = True) -> Statements:
        """Return DML statements for this table's data diff.

        When *safe* is True, DELETE statements are omitted.
        """
        result = Statements()
        # Upserts for inserts and updates (combined via ON CONFLICT)
        for row in self.inserts:
            result.append(self._upsert_statement(row))
        for row in self.updates:
            result.append(self._upsert_statement(row))
        # Deletes only when unsafe mode
        if not safe:
            for row in self.deletes:
                result.append(self._delete_statement(row))
        return result


def _compute_table_diff(
    source_rows: list[dict[str, Any]],
    target_rows: list[dict[str, Any]],
    pk_columns: list[str],
    compare_columns: list[str],
    all_columns: list[str],
    non_pk_columns: list[str],
    schema: str,
    table_name: str,
) -> TableDataDiff:
    """Compare rows and produce a :class:`TableDataDiff`."""

    def pk_tuple(row: dict[str, Any]) -> tuple[Any, ...]:
        return tuple(row[c] for c in pk_columns)

    source_by_pk = {pk_tuple(r): r for r in source_rows}
    target_by_pk = {pk_tuple(r): r for r in target_rows}

    inserts: list[dict[str, Any]] = []
    updates: list[dict[str, Any]] = []
    deletes: list[dict[str, Any]] = []

    # Rows in target but not source → insert
    for pk, row in target_by_pk.items():
        if pk not in source_by_pk:
            inserts.append(row)

    # Rows in both → check for updates (only on compare_columns)
    for pk, target_row in target_by_pk.items():
        if pk in source_by_pk:
            source_row = source_by_pk[pk]
            if any(source_row.get(c) != target_row.get(c) for c in compare_columns):
                updates.append(target_row)

    # Rows in source but not target → delete
    for pk, row in source_by_pk.items():
        if pk not in target_by_pk:
            deletes.append(row)

    return TableDataDiff(
        schema=schema,
        table_name=table_name,
        pk_columns=pk_columns,
        all_columns=all_columns,
        non_pk_columns=non_pk_columns,
        inserts=inserts,
        updates=updates,
        deletes=deletes,
    )


# ---------------------------------------------------------------------------
# Top-level entry point
# ---------------------------------------------------------------------------


def get_data_diff_statements(
    conn_from: Any,
    conn_target: Any,
    inspector_from: Any,
    inspector_target: Any,
    safe: bool = True,
) -> Statements:
    """Generate DML statements for all data-diff annotated tables.

    Scans both inspectors for tables with the ``pgmigra:data-diff`` annotation,
    computes row-level diffs, and returns ordered DML statements.
    """
    # Discover annotated tables from *target* (desired state drives the diff)
    annotated: dict[str, DataDiffAnnotation] = {}
    tables_to_diff: list[tuple[str, str, str]] = []  # (quoted_name, schema, name)

    for key, selectable in inspector_target.tables.items():
        ann = parse_data_diff_annotation(selectable.comment)
        if ann is not None:
            annotated[key] = ann
            tables_to_diff.append((key, selectable.schema, selectable.name))

    # Also check source for tables that exist in source but were removed in target
    # (this is relevant for DELETE generation when the annotation was on the source)
    for key, selectable in inspector_from.tables.items():
        if key not in annotated:
            ann = parse_data_diff_annotation(selectable.comment)
            if ann is not None:
                annotated[key] = ann
                tables_to_diff.append((key, selectable.schema, selectable.name))

    if not annotated:
        return Statements()

    # Build FK dependency graph between annotated tables for ordering
    fk_graph: dict[str, set[str]] = {key: set() for key, _, _ in tables_to_diff}
    for c in inspector_target.constraints.values():
        if c.is_fk:
            t = c.quoted_full_table_name
            ft = c.quoted_full_foreign_table_name
            if t in fk_graph and ft in fk_graph:
                fk_graph[t].add(ft)

    try:
        ordered_keys = list(TopologicalSorter(fk_graph).static_order())
    except Exception:
        ordered_keys = [key for key, _, _ in tables_to_diff]

    # Compute diffs for each table
    all_diffs: dict[str, TableDataDiff] = {}
    for key, schema, name in tables_to_diff:
        ann = annotated[key]

        # Need PK from target (or source if table removed)
        if key in inspector_target.tables:
            pk_cols = _pk_columns_for_table(name, schema, inspector_target)
        else:
            pk_cols = _pk_columns_for_table(name, schema, inspector_from)

        if pk_cols is None:
            continue  # skip tables without PK

        # Determine columns to fetch
        if key in inspector_target.tables:
            all_col_names = list(inspector_target.tables[key].columns.keys())
        elif key in inspector_from.tables:
            all_col_names = list(inspector_from.tables[key].columns.keys())
        else:
            continue

        non_pk_cols = [c for c in all_col_names if c not in pk_cols]
        compare_cols = [c for c in non_pk_cols if c not in ann.ignore]

        # Fetch rows from both databases
        source_rows: list[dict[str, Any]] = []
        target_rows: list[dict[str, Any]] = []

        if key in inspector_from.tables and conn_from is not None:
            source_rows = _fetch_rows(conn_from, schema, name, all_col_names, pk_cols)
            if len(source_rows) > MAX_DATA_DIFF_ROWS:
                continue  # skip huge tables

        if key in inspector_target.tables and conn_target is not None:
            target_rows = _fetch_rows(conn_target, schema, name, all_col_names, pk_cols)
            if len(target_rows) > MAX_DATA_DIFF_ROWS:
                continue  # skip huge tables

        diff = _compute_table_diff(
            source_rows=source_rows,
            target_rows=target_rows,
            pk_columns=pk_cols,
            compare_columns=compare_cols,
            all_columns=all_col_names,
            non_pk_columns=non_pk_cols,
            schema=schema,
            table_name=name,
        )
        all_diffs[key] = diff

    # Emit statements in FK-safe order
    # For inserts/updates: parent tables first (topo order)
    # For deletes: child tables first (reverse topo order)
    statements = Statements()

    # Inserts and updates in dependency order
    for key in ordered_keys:
        if key in all_diffs:
            diff = all_diffs[key]
            for row in diff.inserts:
                statements.append(diff._upsert_statement(row))
            for row in diff.updates:
                statements.append(diff._upsert_statement(row))

    # Deletes in reverse dependency order
    if not safe:
        for key in reversed(ordered_keys):
            if key in all_diffs:
                diff = all_diffs[key]
                for row in diff.deletes:
                    statements.append(diff._delete_statement(row))

    return statements
