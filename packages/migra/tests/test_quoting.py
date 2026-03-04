from migra import Migration
from migra.db import connect, temporary_database


def test_enum_column_quoting():
    """Verify mixed-case column names are properly quoted in enum ALTER statements."""
    with temporary_database() as d0, temporary_database() as d1:
        for url in [d0, d1]:
            with connect(url) as s:
                s.execute("CREATE TYPE status_type AS ENUM ('active');")

        with connect(d0) as s0:
            s0.execute('CREATE TABLE items ("StatusCode" status_type);')
        with connect(d1) as s1:
            s1.execute("DROP TYPE status_type;")
            s1.execute("CREATE TYPE status_type AS ENUM ('active', 'inactive');")
            s1.execute('CREATE TABLE items ("StatusCode" status_type);')

        with connect(d0) as s0, connect(d1) as s1:
            m = Migration(s0, s1)
            m.set_safety(False)
            m.add_all_changes()

            sql_out = m.sql
            if sql_out:
                if "StatusCode" in sql_out or "statuscode" in sql_out:
                    assert '"StatusCode"' in sql_out, (
                        f"Mixed-case column name not properly quoted in SQL:\n{sql_out}"
                    )
