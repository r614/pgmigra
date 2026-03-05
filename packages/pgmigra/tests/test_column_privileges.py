from pgmigra.changes import Changes
from pgmigra.db import connect, temporary_database
from pgmigra.schemainspect import get_inspector
from pgmigra.schemainspect.pg.objects import InspectedPrivilege

from .conftest import create_role, schemainspect_test_role

SETUP_TABLE = "CREATE TABLE t1 (id int primary key, name text, val text);"


def test_column_privilege_inspect(db):
    with connect(db) as s:
        create_role(s, schemainspect_test_role)
        s.execute(SETUP_TABLE)
        s.execute(f"GRANT SELECT (id, name) ON t1 TO {schemainspect_test_role};")
        i = get_inspector(s)

        col_privs = [p for p in i.privileges.values() if p.columns is not None]
        assert len(col_privs) >= 1

        found = False
        for p in col_privs:
            if p.name == "t1" and p.privilege == "select":
                found = True
                assert sorted(p.columns) == ["id", "name"]
                break
        assert found


def test_column_privilege_create_statement(db):
    with connect(db) as s:
        create_role(s, schemainspect_test_role)
        s.execute(SETUP_TABLE)
        s.execute(f"GRANT SELECT (id, name) ON t1 TO {schemainspect_test_role};")
        i = get_inspector(s)

        col_privs = [
            p for p in i.privileges.values() if p.columns is not None and p.name == "t1"
        ]
        assert len(col_privs) >= 1
        stmt = col_privs[0].create_statement
        assert "grant select" in stmt
        assert '"id"' in stmt
        assert '"name"' in stmt


def test_column_privilege_drop_statement(db):
    with connect(db) as s:
        create_role(s, schemainspect_test_role)
        s.execute(SETUP_TABLE)
        s.execute(f"GRANT UPDATE (name, val) ON t1 TO {schemainspect_test_role};")
        i = get_inspector(s)

        col_privs = [
            p
            for p in i.privileges.values()
            if p.columns is not None and p.name == "t1" and p.privilege == "update"
        ]
        assert len(col_privs) >= 1
        stmt = col_privs[0].drop_statement
        assert "revoke update" in stmt
        assert '"name"' in stmt
        assert '"val"' in stmt


def test_column_privilege_key_distinct():
    a = InspectedPrivilege("column", "public", "t1", "select", "user1", ["id"])
    b = InspectedPrivilege("column", "public", "t1", "select", "user1", ["id", "name"])
    assert a.key != b.key
    assert a != b


def test_column_privilege_equality():
    a = InspectedPrivilege("column", "public", "t1", "select", "user1", ["id", "name"])
    a2 = InspectedPrivilege("column", "public", "t1", "select", "user1", ["name", "id"])
    assert a == a2

    b = InspectedPrivilege("table", "public", "t1", "select", "user1")
    assert a != b


def test_column_privilege_not_confused_with_table_grant(db):
    with connect(db) as s:
        create_role(s, schemainspect_test_role)
        s.execute(SETUP_TABLE)
        s.execute(f"GRANT SELECT ON t1 TO {schemainspect_test_role};")
        i = get_inspector(s)

        table_privs = [
            p
            for p in i.privileges.values()
            if p.name == "t1" and p.privilege == "select" and p.columns is None
        ]
        col_privs = [
            p
            for p in i.privileges.values()
            if p.name == "t1" and p.privilege == "select" and p.columns is not None
        ]
        assert len(table_privs) == 1
        assert len(col_privs) == 0


def test_column_privilege_diff_add():
    with temporary_database() as d1, temporary_database() as d2:
        with connect(d1) as s1, connect(d2) as s2:
            for s in (s1, s2):
                create_role(s, schemainspect_test_role)
                s.execute(SETUP_TABLE)
            s2.execute(f"GRANT SELECT (id) ON t1 TO {schemainspect_test_role};")
            i_from = get_inspector(s1)
            i_target = get_inspector(s2)

        changes = Changes(i_from, i_target)
        stmts = changes.privileges(creations_only=True)
        sql = stmts.sql
        assert "grant select" in sql
        assert '"id"' in sql


def test_column_privilege_diff_drop():
    with temporary_database() as d1, temporary_database() as d2:
        with connect(d1) as s1, connect(d2) as s2:
            for s in (s1, s2):
                create_role(s, schemainspect_test_role)
                s.execute(SETUP_TABLE)
            s1.execute(f"GRANT UPDATE (name) ON t1 TO {schemainspect_test_role};")
            i_from = get_inspector(s1)
            i_target = get_inspector(s2)

        changes = Changes(i_from, i_target)
        stmts = changes.privileges(drops_only=True)
        stmts.safe = False
        sql = stmts.sql
        assert "revoke update" in sql
        assert '"name"' in sql
