from migra import Migration
from migra.db import connect, temporary_database
from migra.schemainspect import get_inspector


def test_generated_column_type():
    """Verify generated columns track generated_type and produce correct DDL."""
    with (
        temporary_database(host="localhost") as d0,
        temporary_database(host="localhost") as d1,
    ):
        with connect(d0) as s0, connect(d1) as s1:
            s0.execute("create table t(a int);")
            s1.execute("""
                create table t(
                    a int,
                    b int generated always as (a * 2) stored
                );
            """)

        with connect(d0) as s0, connect(d1) as s1:
            m = Migration(s0, s1)
            m.inspect_from()
            m.inspect_target()
            m.set_safety(False)
            m.add_all_changes()
            sql = m.sql.lower()
            assert "generated always as" in sql
            assert "stored" in sql


def test_generated_column_inspected():
    """Verify generated_type is correctly introspected from pg_attribute."""
    with temporary_database(host="localhost") as d0:
        with connect(d0) as s0:
            s0.execute("""
                create table t(
                    a int,
                    b int generated always as (a * 2) stored
                );
            """)
        with connect(d0) as s0:
            i = get_inspector(s0)
            cols = i.relations['"public"."t"'].columns
            assert cols["a"].generated_type is None
            assert cols["b"].generated_type == "s"
            assert cols["b"].is_generated is True


def test_generated_columns_inspect(db):
    with connect(db) as s:
        s.execute(
            """create table t(
                c int generated always as (1) stored
        ) """
        )

        i = get_inspector(s)

        t_key = '"public"."t"'
        assert list(i.tables.keys())[0] == t_key

        t = i.tables[t_key]

        EXPECTED = ("1", False, False, True)

        c = t.columns["c"]

        tup = (c.default, c.is_identity, c.is_identity_always, c.is_generated)

        assert tup == EXPECTED

        EXPECTED = '"c" integer generated always as (1) stored'

        assert c.creation_clause == EXPECTED
