from migra import Migration
from migra.db import connect, temporary_database


def test_domain_diffing():
    """Verify domains are included in migration diffs."""
    with temporary_database() as d0, temporary_database() as d1:
        with connect(d1) as s1:
            s1.execute(
                "CREATE DOMAIN email_address AS text CHECK (VALUE ~ '^[^@]+@[^@]+$');"
            )

        with connect(d0) as s0, connect(d1) as s1:
            m = Migration(s0, s1)
            m.set_safety(False)
            m.add_all_changes()

            sql_out = m.sql
            assert "email_address" in sql_out, (
                f"Domain not found in migration SQL. Got:\n{sql_out}"
            )
            assert "create domain" in sql_out.lower(), (
                f"CREATE DOMAIN not found in migration SQL. Got:\n{sql_out}"
            )


def test_domain_drop():
    """Verify domain drops are generated."""
    with temporary_database() as d0, temporary_database() as d1:
        with connect(d0) as s0:
            s0.execute("CREATE DOMAIN positive_int AS integer CHECK (VALUE > 0);")

        with connect(d0) as s0, connect(d1) as s1:
            m = Migration(s0, s1)
            m.set_safety(False)
            m.add_all_changes()

            sql_out = m.sql
            assert "drop domain" in sql_out.lower(), (
                f"DROP DOMAIN not found in migration SQL. Got:\n{sql_out}"
            )
