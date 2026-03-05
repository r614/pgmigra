from pgmigra import schemainspect
from pgmigra.db import connect
from pgmigra.schemainspect import NullInspector, get_inspector

from ._schemainspect_helpers import asserts_pg, setup_pg_schema


def test_postgres_inspect(db):
    with connect(db) as s:
        setup_pg_schema(s)
        i = get_inspector(s)
        asserts_pg(i)
        assert i == i == get_inspector(s)


def test_empty():
    x = NullInspector()
    assert x.tables == {}
    assert x.relations == {}
    assert type(schemainspect.get_inspector(None)) == NullInspector


def test_raw_connection(db):
    with connect(db) as s:
        setup_pg_schema(s)
        i1 = get_inspector(s)

    with connect(db) as s:
        i2 = get_inspector(s)

    assert i1 == i2
