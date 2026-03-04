from migra import Migration
from migra.db import connect, temporary_database
from migra.schemainspect import get_inspector


def test_range_type_introspection():
    with temporary_database() as d0:
        with connect(d0) as s0:
            s0.execute(
                "CREATE TYPE floatrange AS RANGE (subtype = float8, subtype_diff = float8mi);"
            )

        with connect(d0) as s0:
            i = get_inspector(s0)
            assert len(i.range_types) == 1
            rt = list(i.range_types.values())[0]
            assert rt.name == "floatrange"
            assert rt.schema == "public"
            assert rt.subtype == "double precision"
            assert rt.subtype_diff == "float8mi"
            assert "create type" in rt.create_statement.lower()
            assert "floatrange" in rt.create_statement
            assert "drop type" in rt.drop_statement.lower()


def test_range_type_diffing():
    with temporary_database() as d0, temporary_database() as d1:
        with connect(d1) as s1:
            s1.execute("CREATE TYPE floatrange AS RANGE (subtype = float8);")

        with connect(d0) as s0, connect(d1) as s1:
            m = Migration(s0, s1)
            m.set_safety(False)
            m.add_all_changes()
            sql_out = m.sql
            assert "floatrange" in sql_out
            assert "create type" in sql_out.lower()


def test_range_type_drop():
    with temporary_database() as d0, temporary_database() as d1:
        with connect(d0) as s0:
            s0.execute("CREATE TYPE floatrange AS RANGE (subtype = float8);")

        with connect(d0) as s0, connect(d1) as s1:
            m = Migration(s0, s1)
            m.set_safety(False)
            m.add_all_changes()
            sql_out = m.sql
            assert "drop type" in sql_out.lower()
            assert "floatrange" in sql_out


def test_range_type_no_change():
    with temporary_database() as d0, temporary_database() as d1:
        with connect(d0) as s0:
            s0.execute("CREATE TYPE floatrange AS RANGE (subtype = float8);")
        with connect(d1) as s1:
            s1.execute("CREATE TYPE floatrange AS RANGE (subtype = float8);")

        with connect(d0) as s0, connect(d1) as s1:
            m = Migration(s0, s1)
            m.set_safety(False)
            m.add_all_changes()
            assert m.sql.strip() == ""


def test_multirange_column():
    with temporary_database() as d0, temporary_database() as d1:
        with connect(d1) as s1:
            s1.execute("CREATE TYPE floatrange AS RANGE (subtype = float8);")
            s1.execute(
                "CREATE TABLE measurements (id serial primary key, ranges floatmultirange);"
            )

        with connect(d0) as s0, connect(d1) as s1:
            m = Migration(s0, s1)
            m.set_safety(False)
            m.add_all_changes()
            sql_out = m.sql
            assert "floatrange" in sql_out
            assert "measurements" in sql_out
