from __future__ import annotations

from typing import Any

import psycopg

from .inspector import DBInspector, NullInspector
from .pg import PostgreSQL


def get_inspector(
    x: psycopg.Connection[Any] | None,
    schema: str | None = None,
    exclude_schema: str | None = None,
) -> DBInspector:
    if schema and exclude_schema:
        raise ValueError("Cannot provide both schema and exclude_schema")
    if x is None:
        return NullInspector()

    inspected = PostgreSQL(x)
    if schema:
        inspected.one_schema(schema)
    elif exclude_schema:
        inspected.exclude_schema(exclude_schema)
    return inspected
