from pgmigra import Migration
from pgmigra.db import connect, temporary_database


def test_view_ownership_change():
    """Verify ALTER VIEW ... OWNER TO is generated for ownership changes."""
    test_role = "migra_view_owner_test"

    with temporary_database() as d0, temporary_database() as d1:
        for url in [d0, d1]:
            with connect(url) as s:
                role = s.execute(
                    "SELECT 1 FROM pg_roles WHERE rolname = %s", (test_role,)
                )
                if not list(role):
                    s.execute(f"CREATE ROLE {test_role}")

                s.execute("CREATE TABLE test_data (id serial, value text);")

        with connect(d0) as s0:
            s0.execute("CREATE VIEW test_view AS SELECT id FROM test_data;")

        with connect(d1) as s1:
            s1.execute("CREATE VIEW test_view AS SELECT id, value FROM test_data;")
            s1.execute(f"ALTER VIEW test_view OWNER TO {test_role};")

        with connect(d0) as s0, connect(d1) as s1:
            m = Migration(s0, s1)
            m.set_safety(False)
            m.add_all_changes()

            sql_out = m.sql
            assert "owner to" in sql_out.lower(), (
                f"ALTER ... OWNER TO not generated for view ownership change.\n{sql_out}"
            )
            assert test_role in sql_out.lower() or f'"{test_role}"' in sql_out, (
                f"Target role not found in migration SQL.\n{sql_out}"
            )
