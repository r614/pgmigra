from pgmigra.changes import Changes
from pgmigra.db import connect, temporary_database
from pgmigra.schemainspect import get_inspector

SETUP_FDW = """
CREATE EXTENSION IF NOT EXISTS file_fdw;
CREATE FOREIGN DATA WRAPPER test_fdw_wrapper VALIDATOR postgresql_fdw_validator;
"""

SETUP_SERVER = """
CREATE EXTENSION IF NOT EXISTS file_fdw;
CREATE SERVER test_server FOREIGN DATA WRAPPER file_fdw;
"""


def test_fdw_inspect(db):
    with connect(db) as s:
        s.execute("CREATE EXTENSION IF NOT EXISTS file_fdw;")
        i = get_inspector(s)

        assert '"file_fdw"' in i.fdws
        fdw = i.fdws['"file_fdw"']
        assert fdw.name == "file_fdw"
        assert "CREATE FOREIGN DATA WRAPPER" in fdw.create_statement
        assert "DROP FOREIGN DATA WRAPPER" in fdw.drop_statement


def test_fdw_with_validator(db):
    with connect(db) as s:
        s.execute(SETUP_FDW)
        i = get_inspector(s)

        fdw = i.fdws['"test_fdw_wrapper"']
        assert fdw.name == "test_fdw_wrapper"
        assert fdw.validator_name == "postgresql_fdw_validator"
        assert "VALIDATOR" in fdw.create_statement


def test_fdw_diff_add():
    with temporary_database() as d1, temporary_database() as d2:
        with connect(d1) as s1, connect(d2) as s2:
            s2.execute("CREATE EXTENSION IF NOT EXISTS file_fdw;")
            s2.execute(
                "CREATE FOREIGN DATA WRAPPER new_fdw VALIDATOR postgresql_fdw_validator;"
            )
            i_from = get_inspector(s1)
            i_target = get_inspector(s2)

        changes = Changes(i_from, i_target)
        stmts = changes.fdws(creations_only=True)
        sql = stmts.sql
        assert "CREATE FOREIGN DATA WRAPPER" in sql
        assert "new_fdw" in sql


def test_fdw_diff_drop():
    with temporary_database() as d1, temporary_database() as d2:
        with connect(d1) as s1, connect(d2) as s2:
            s1.execute("CREATE EXTENSION IF NOT EXISTS file_fdw;")
            s1.execute(
                "CREATE FOREIGN DATA WRAPPER old_fdw VALIDATOR postgresql_fdw_validator;"
            )
            i_from = get_inspector(s1)
            i_target = get_inspector(s2)

        changes = Changes(i_from, i_target)
        stmts = changes.fdws(drops_only=True)
        stmts.safe = False
        sql = stmts.sql
        assert "DROP FOREIGN DATA WRAPPER" in sql
        assert "old_fdw" in sql


def test_fdw_no_diff_when_equal():
    with temporary_database() as d1, temporary_database() as d2:
        with connect(d1) as s1, connect(d2) as s2:
            s1.execute("CREATE EXTENSION IF NOT EXISTS file_fdw;")
            s2.execute("CREATE EXTENSION IF NOT EXISTS file_fdw;")
            i_from = get_inspector(s1)
            i_target = get_inspector(s2)

        changes = Changes(i_from, i_target)
        stmts = changes.fdws()
        assert len(stmts) == 0


def test_foreign_server_inspect(db):
    with connect(db) as s:
        s.execute(SETUP_SERVER)
        i = get_inspector(s)

        assert '"test_server"' in i.foreign_servers
        srv = i.foreign_servers['"test_server"']
        assert srv.name == "test_server"
        assert srv.fdw_name == "file_fdw"
        assert "CREATE SERVER" in srv.create_statement
        assert "FOREIGN DATA WRAPPER" in srv.create_statement
        assert "DROP SERVER" in srv.drop_statement


def test_foreign_server_with_options(db):
    with connect(db) as s:
        s.execute("CREATE EXTENSION IF NOT EXISTS file_fdw;")
        s.execute(
            "CREATE SERVER opt_server TYPE 'test' VERSION '1.0' FOREIGN DATA WRAPPER file_fdw;"
        )
        i = get_inspector(s)

        srv = i.foreign_servers['"opt_server"']
        assert srv.server_type == "test"
        assert srv.server_version == "1.0"
        assert "TYPE" in srv.create_statement
        assert "VERSION" in srv.create_statement


def test_foreign_server_diff_add():
    with temporary_database() as d1, temporary_database() as d2:
        with connect(d1) as s1, connect(d2) as s2:
            s2.execute("CREATE EXTENSION IF NOT EXISTS file_fdw;")
            s2.execute("CREATE SERVER new_srv FOREIGN DATA WRAPPER file_fdw;")
            i_from = get_inspector(s1)
            i_target = get_inspector(s2)

        changes = Changes(i_from, i_target)
        stmts = changes.foreign_servers(creations_only=True)
        sql = stmts.sql
        assert "CREATE SERVER" in sql
        assert "new_srv" in sql


def test_foreign_server_diff_drop():
    with temporary_database() as d1, temporary_database() as d2:
        with connect(d1) as s1, connect(d2) as s2:
            s1.execute("CREATE EXTENSION IF NOT EXISTS file_fdw;")
            s1.execute("CREATE SERVER old_srv FOREIGN DATA WRAPPER file_fdw;")
            i_from = get_inspector(s1)
            i_target = get_inspector(s2)

        changes = Changes(i_from, i_target)
        stmts = changes.foreign_servers(drops_only=True)
        stmts.safe = False
        sql = stmts.sql
        assert "DROP SERVER" in sql
        assert "old_srv" in sql


def test_user_mapping_inspect(db):
    with connect(db) as s:
        s.execute(SETUP_SERVER)
        s.execute("CREATE USER MAPPING FOR CURRENT_USER SERVER test_server;")
        i = get_inspector(s)

        assert len(i.user_mappings) >= 1
        found = False
        for m in i.user_mappings.values():
            if m.server_name == "test_server":
                found = True
                assert "CREATE USER MAPPING" in m.create_statement
                assert "DROP USER MAPPING" in m.drop_statement
                break
        assert found


def test_user_mapping_diff_add():
    with temporary_database() as d1, temporary_database() as d2:
        with connect(d1) as s1, connect(d2) as s2:
            s1.execute(SETUP_SERVER)
            s2.execute(SETUP_SERVER)
            s2.execute("CREATE USER MAPPING FOR PUBLIC SERVER test_server;")
            i_from = get_inspector(s1)
            i_target = get_inspector(s2)

        changes = Changes(i_from, i_target)
        stmts = changes.user_mappings(creations_only=True)
        sql = stmts.sql
        assert "CREATE USER MAPPING" in sql


def test_user_mapping_diff_drop():
    with temporary_database() as d1, temporary_database() as d2:
        with connect(d1) as s1, connect(d2) as s2:
            s1.execute(SETUP_SERVER)
            s2.execute(SETUP_SERVER)
            s1.execute("CREATE USER MAPPING FOR PUBLIC SERVER test_server;")
            i_from = get_inspector(s1)
            i_target = get_inspector(s2)

        changes = Changes(i_from, i_target)
        stmts = changes.user_mappings(drops_only=True)
        stmts.safe = False
        sql = stmts.sql
        assert "DROP USER MAPPING" in sql
