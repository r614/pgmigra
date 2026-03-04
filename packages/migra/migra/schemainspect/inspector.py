from __future__ import annotations

import datetime
import decimal
import uuid
from typing import Any

PG_TYPE_MAP: dict[str, type[Any]] = {
    # numeric
    "bigint": int,
    "integer": int,
    "smallint": int,
    "numeric": decimal.Decimal,
    "real": float,
    "double precision": float,
    "float": float,
    "boolean": bool,
    "oid": int,
    # string
    "text": str,
    "character varying": str,
    "character": str,
    "varchar": str,
    "char": str,
    '"char"': str,
    "name": str,
    "citext": str,
    # binary
    "bytea": bytes,
    # date/time
    "date": datetime.date,
    "timestamp": datetime.datetime,
    "timestamp without time zone": datetime.datetime,
    "timestamp with time zone": datetime.datetime,
    "time": datetime.time,
    "time without time zone": datetime.time,
    "time with time zone": datetime.time,
    "interval": datetime.timedelta,
    # json
    "json": dict,
    "jsonb": dict,
    # uuid
    "uuid": uuid.UUID,
    # network
    "inet": str,
    "cidr": str,
    "macaddr": str,
    "macaddr8": str,
    # monetary
    "money": str,
    # xml
    "xml": str,
    # bit
    "bit": str,
    "bit varying": str,
    # text search
    "tsvector": str,
    "tsquery": str,
    # geometric
    "point": str,
    "line": str,
    "lseg": str,
    "box": str,
    "path": str,
    "polygon": str,
    "circle": str,
    # range types
    "int4range": str,
    "int8range": str,
    "numrange": str,
    "daterange": str,
    "tsrange": str,
    "tstzrange": str,
    # multirange types (PG 14+)
    "int4multirange": str,
    "int8multirange": str,
    "nummultirange": str,
    "datemultirange": str,
    "tsmultirange": str,
    "tstzmultirange": str,
    # other
    "regclass": str,
    "hstore": dict,
}


def to_pytype(typename: str) -> type[Any]:
    return PG_TYPE_MAP.get(typename, type(None))


class NullInspector:
    def __init__(self) -> None:
        pass

    def __getattr__(self, name: str) -> dict[str, Any]:
        return {}
