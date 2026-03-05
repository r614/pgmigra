from pgmigra.db import connect
from pgmigra.schemainspect import get_inspector


def test_weird_names(db):
    with connect(db) as s:
        s.execute("""create table "a(abc=3)"(id text)  """)
        i = get_inspector(s)
        assert list(i.tables.keys())[0] == '"public"."a(abc=3)"'
