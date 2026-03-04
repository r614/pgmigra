from migra import Migration
from migra.command import parse_args
from migra.db import connect, temporary_database


def test_multiple_schemas_cli():
    """Verify --schema can be specified multiple times."""
    args = parse_args(
        ["--schema", "s1", "--schema", "s2", "--unsafe", "EMPTY", "EMPTY"]
    )
    assert args.schema == ["s1", "s2"]


def test_multiple_schemas_migration():
    """Verify multi-schema diffing works."""
    with temporary_database() as d0, temporary_database() as d1:
        with connect(d1) as s1:
            s1.execute("CREATE SCHEMA app;")
            s1.execute("CREATE SCHEMA api;")
            s1.execute("CREATE TABLE app.users (id serial PRIMARY KEY, name text);")
            s1.execute("CREATE TABLE api.endpoints (id serial PRIMARY KEY, path text);")

        with connect(d0) as s0, connect(d1) as s1:
            m = Migration(s0, s1, schema=["app", "api"])
            m.set_safety(False)
            m.add_all_changes()

            sql_out = m.sql
            assert "app" in sql_out, f"Schema 'app' not in output.\n{sql_out}"
            assert "api" in sql_out, f"Schema 'api' not in output.\n{sql_out}"
            assert "users" in sql_out, f"Table 'users' not in output.\n{sql_out}"
            assert "endpoints" in sql_out, (
                f"Table 'endpoints' not in output.\n{sql_out}"
            )


def test_single_schema_backward_compat():
    """Verify single --schema still works with new list behavior."""
    args = parse_args(["--schema", "public", "--unsafe", "EMPTY", "EMPTY"])
    assert args.schema == ["public"]
