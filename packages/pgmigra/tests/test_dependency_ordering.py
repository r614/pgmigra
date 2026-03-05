from pgmigra import Migration
from pgmigra.db import connect, temporary_database


def test_dependency_ordering_creates():
    """Verify tables are created in dependency order (parent before child).

    Uses table inheritance (INHERITS) because schemainspect populates
    dependent_on/dependents for inherited tables via parent_table, which is
    what get_table_changes uses for its TopologicalSorter graph.
    """
    with temporary_database() as d0, temporary_database() as d1:
        with connect(d1) as s1:
            s1.execute("""
                CREATE TABLE parent (
                    id serial PRIMARY KEY,
                    name text
                );
                CREATE TABLE child (
                    id serial,
                    value text
                ) INHERITS (parent);
            """)

        with connect(d0) as s0, connect(d1) as s1:
            m = Migration(s0, s1)
            m.set_safety(False)
            m.add_all_changes()

            sql_out = m.sql
            parent_pos = sql_out.lower().find('create table "public"."parent"')
            child_pos = sql_out.lower().find('create table "public"."child"')
            assert parent_pos >= 0, f"parent table CREATE not found.\n{sql_out}"
            assert child_pos >= 0, f"child table CREATE not found.\n{sql_out}"
            assert parent_pos < child_pos, (
                f"parent table must be created before child table.\n{sql_out}"
            )


def test_dependency_ordering_drops():
    """Verify tables are dropped in reverse dependency order (child before parent).

    Uses table inheritance (INHERITS) because schemainspect populates
    dependent_on/dependents for inherited tables via parent_table, which is
    what get_table_changes uses for its TopologicalSorter graph.
    """
    with temporary_database() as d0, temporary_database() as d1:
        with connect(d0) as s0:
            s0.execute("""
                CREATE TABLE parent (
                    id serial PRIMARY KEY,
                    name text
                );
                CREATE TABLE child (
                    id serial,
                    value text
                ) INHERITS (parent);
            """)

        with connect(d0) as s0, connect(d1) as s1:
            m = Migration(s0, s1)
            m.set_safety(False)
            m.add_all_changes()

            sql_out = m.sql
            assert "drop table" in sql_out.lower(), f"No DROP TABLE found.\n{sql_out}"
            child_pos = sql_out.lower().find('drop table "public"."child"')
            parent_pos = sql_out.lower().find('drop table "public"."parent"')
            assert child_pos >= 0, f"child table DROP not found.\n{sql_out}"
            assert parent_pos >= 0, f"parent table DROP not found.\n{sql_out}"
            assert child_pos < parent_pos, (
                f"child table must be dropped before parent table.\n{sql_out}"
            )
