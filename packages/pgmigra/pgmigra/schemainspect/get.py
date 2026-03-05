from __future__ import annotations

from typing import Any

import psycopg

from .inspector import NullInspector
from .pg import PostgreSQL


def get_inspector(
    x: psycopg.Connection[Any] | None,
    schema: str | list[str] | None = None,
    exclude_schema: str | None = None,
) -> PostgreSQL | NullInspector:
    if schema and exclude_schema:
        raise ValueError("Cannot provide both schema and exclude_schema")
    if x is None:
        return NullInspector()

    inspected = PostgreSQL(x)
    if schema:
        if isinstance(schema, list):
            if len(schema) == 1:
                inspected.one_schema(schema[0])
            else:
                inspected.filter_schemas(schema)
        else:
            inspected.one_schema(schema)
    elif exclude_schema:
        inspected.exclude_schema(exclude_schema)
    return inspected
