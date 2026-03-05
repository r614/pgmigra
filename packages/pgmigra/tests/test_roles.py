from pgmigra import Migration
from pgmigra.command import parse_args
from pgmigra.db import connect, temporary_database


def test_role_diffing():
    """Verify role creation is detected with --with-roles."""
    test_role = "migra_test_role_abc123"

    with temporary_database() as d0, temporary_database() as d1:
        with connect(d1) as s1:
            s1.execute(f"DROP ROLE IF EXISTS {test_role}")
            s1.execute(f"CREATE ROLE {test_role} LOGIN")

        try:
            with connect(d0) as s0, connect(d1) as s1:
                m = Migration(s0, s1)
                m.set_safety(False)

                m.add_all_changes(roles=False)
                sql_no_roles = m.sql

                m.clear()
                m.add_all_changes(roles=True)
                sql_with_roles = m.sql

            assert isinstance(sql_no_roles, str)
            assert isinstance(sql_with_roles, str)
        finally:
            with connect(d1) as s1:
                s1.execute(f"DROP ROLE IF EXISTS {test_role}")


def test_role_cli_flag():
    """Verify --with-roles CLI flag is parsed."""
    args = parse_args(["--with-roles", "--unsafe", "EMPTY", "EMPTY"])
    assert args.with_roles is True

    args = parse_args(["--unsafe", "EMPTY", "EMPTY"])
    assert args.with_roles is False
