from migra.changes import Changes
from migra.db import connect, temporary_database
from migra.schemainspect import get_inspector

SETUP_TYPES = """
CREATE TYPE my_type AS (x int, y int);
"""

SETUP_FUNC = """
CREATE FUNCTION text_to_my_type(text) RETURNS my_type
  LANGUAGE plpgsql IMMUTABLE AS
$$
DECLARE
  result my_type;
BEGIN
  result.x := 0;
  result.y := 0;
  RETURN result;
END;
$$;
"""


def test_cast_with_function(db):
    with connect(db) as s:
        s.execute(SETUP_TYPES)
        s.execute(SETUP_FUNC)
        s.execute("CREATE CAST (text AS my_type) WITH FUNCTION text_to_my_type(text);")
        i = get_inspector(s)

        key = "(text AS my_type)"
        assert key in i.casts
        c = i.casts[key]
        assert c.source_type == "text"
        assert c.target_type == "my_type"
        assert c.method == "f"
        assert c.function_name == "text_to_my_type"
        assert "WITH FUNCTION" in c.create_statement


def test_cast_with_inout(db):
    with connect(db) as s:
        s.execute(SETUP_TYPES)
        s.execute("CREATE CAST (text AS my_type) WITH INOUT AS IMPLICIT;")
        i = get_inspector(s)

        key = "(text AS my_type)"
        assert key in i.casts
        c = i.casts[key]
        assert c.method == "i"
        assert c.context == "i"
        assert "WITH INOUT" in c.create_statement
        assert "AS IMPLICIT" in c.create_statement


def test_cast_assignment_context(db):
    with connect(db) as s:
        s.execute(SETUP_TYPES)
        s.execute(SETUP_FUNC)
        s.execute(
            "CREATE CAST (text AS my_type) WITH FUNCTION text_to_my_type(text) AS ASSIGNMENT;"
        )
        i = get_inspector(s)

        key = "(text AS my_type)"
        c = i.casts[key]
        assert c.context == "a"
        assert "AS ASSIGNMENT" in c.create_statement


def test_cast_roundtrip(db):
    with connect(db) as s:
        s.execute(SETUP_TYPES)
        s.execute(SETUP_FUNC)
        s.execute("CREATE CAST (text AS my_type) WITH FUNCTION text_to_my_type(text);")
        i = get_inspector(s)
        c = i.casts["(text AS my_type)"]
        create_sql = c.create_statement
        drop_sql = c.drop_statement

        s.execute(drop_sql)
        i2 = get_inspector(s)
        assert "(text AS my_type)" not in i2.casts

        s.execute(create_sql)
        i3 = get_inspector(s)
        c3 = i3.casts["(text AS my_type)"]
        assert c == c3


def test_cast_diff_add():
    with temporary_database() as d1, temporary_database() as d2:
        with connect(d1) as s1, connect(d2) as s2:
            s1.execute(SETUP_TYPES)
            s1.execute(SETUP_FUNC)
            s2.execute(SETUP_TYPES)
            s2.execute(SETUP_FUNC)
            s2.execute(
                "CREATE CAST (text AS my_type) WITH FUNCTION text_to_my_type(text);"
            )
            i_from = get_inspector(s1)
            i_target = get_inspector(s2)

        changes = Changes(i_from, i_target)
        stmts = changes.casts(creations_only=True)
        sql = stmts.sql
        assert "CREATE CAST" in sql


def test_cast_diff_drop():
    with temporary_database() as d1, temporary_database() as d2:
        with connect(d1) as s1, connect(d2) as s2:
            s1.execute(SETUP_TYPES)
            s1.execute(SETUP_FUNC)
            s2.execute(SETUP_TYPES)
            s2.execute(SETUP_FUNC)
            s1.execute(
                "CREATE CAST (text AS my_type) WITH FUNCTION text_to_my_type(text);"
            )
            i_from = get_inspector(s1)
            i_target = get_inspector(s2)

        changes = Changes(i_from, i_target)
        stmts = changes.casts(drops_only=True)
        stmts.safe = False
        sql = stmts.sql
        assert "DROP CAST" in sql


def test_cast_no_diff_when_equal():
    with temporary_database() as d1, temporary_database() as d2:
        with connect(d1) as s1, connect(d2) as s2:
            ddl = (
                SETUP_TYPES
                + SETUP_FUNC
                + "CREATE CAST (text AS my_type) WITH FUNCTION text_to_my_type(text);"
            )
            s1.execute(ddl)
            s2.execute(ddl)
            i_from = get_inspector(s1)
            i_target = get_inspector(s2)

        changes = Changes(i_from, i_target)
        stmts = changes.casts()
        assert len(stmts) == 0
