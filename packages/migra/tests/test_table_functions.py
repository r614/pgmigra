from migra import Migration
from migra.db import connect, temporary_database


def test_table_returning_function_change():
    """Verify functions with changed RETURNS TABLE get DROP + CREATE."""
    with temporary_database() as d0, temporary_database() as d1:
        with connect(d0) as s0:
            s0.execute("""
                CREATE FUNCTION get_users()
                RETURNS TABLE(id integer, name text)
                LANGUAGE sql STABLE AS
                'SELECT 1, ''test''::text';
            """)
        with connect(d1) as s1:
            s1.execute("""
                CREATE FUNCTION get_users()
                RETURNS TABLE(id integer, name text, email text)
                LANGUAGE sql STABLE AS
                'SELECT 1, ''test''::text, ''test@test.com''::text';
            """)

        with connect(d0) as s0, connect(d1) as s1:
            m = Migration(s0, s1)
            m.set_safety(False)
            m.add_all_changes()

            sql_out = m.sql
            assert "drop function" in sql_out.lower(), (
                f"DROP FUNCTION not found - TABLE-returning function change not detected.\n{sql_out}"
            )
            assert "get_users" in sql_out, (
                f"get_users not found in migration SQL.\n{sql_out}"
            )
