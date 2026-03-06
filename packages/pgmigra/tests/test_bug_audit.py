"""Tests to verify reported bugs from the codebase audit.

Each test demonstrates whether a reported bug is real by exercising
the specific code path and checking for the expected (broken) behavior.
"""

from uuid import uuid4

from pgmigra.changes import Changes
from pgmigra.db import connect, temporary_database
from pgmigra.schemainspect import get_inspector
from pgmigra.schemainspect.pg.objects.publication import InspectedPublication
from pgmigra.schemainspect.pg.objects.role import InspectedRole
from pgmigra.schemainspect.pg.objects.user_mapping import InspectedUserMapping

# -- BUG 1: Role options_clause only emits positive flags --------------------


def test_role_revoke_superuser():
    """Revoking SUPERUSER should produce ALTER ROLE ... NOSUPERUSER."""
    r_from = InspectedRole(
        name="r", superuser=True, inherit=True, createrole=False,
        createdb=False, login=True, replication=False, bypassrls=False,
        connlimit=-1, member_of=[],
    )
    r_target = InspectedRole(
        name="r", superuser=False, inherit=True, createrole=False,
        createdb=False, login=True, replication=False, bypassrls=False,
        connlimit=-1, member_of=[],
    )
    assert r_from != r_target
    stmts = r_target.alter_statements(r_from)
    sql = " ".join(stmts).lower()
    assert "nosuperuser" in sql, (
        f"BUG: revoking SUPERUSER does not produce NOSUPERUSER. Got: {sql}"
    )


def test_role_revoke_createdb():
    r_from = InspectedRole(
        name="r", superuser=False, inherit=True, createrole=False,
        createdb=True, login=True, replication=False, bypassrls=False,
        connlimit=-1, member_of=[],
    )
    r_target = InspectedRole(
        name="r", superuser=False, inherit=True, createrole=False,
        createdb=False, login=True, replication=False, bypassrls=False,
        connlimit=-1, member_of=[],
    )
    stmts = r_target.alter_statements(r_from)
    sql = " ".join(stmts).lower()
    assert "nocreatedb" in sql, (
        f"BUG: revoking CREATEDB does not produce NOCREATEDB. Got: {sql}"
    )


def test_role_revoke_login():
    r_from = InspectedRole(
        name="r", superuser=False, inherit=True, createrole=False,
        createdb=False, login=True, replication=False, bypassrls=False,
        connlimit=-1, member_of=[],
    )
    r_target = InspectedRole(
        name="r", superuser=False, inherit=True, createrole=False,
        createdb=False, login=False, replication=False, bypassrls=False,
        connlimit=-1, member_of=[],
    )
    stmts = r_target.alter_statements(r_from)
    sql = " ".join(stmts).lower()
    assert "nologin" in sql, (
        f"BUG: revoking LOGIN does not produce NOLOGIN. Got: {sql}"
    )


# -- BUG 2: Extension alter_statements returns [None] -----------------------


def test_extension_version_to_none():
    """Extension with version diffing against one without should not crash."""
    from pgmigra.schemainspect.pg.objects.extension import InspectedExtension

    e_from = InspectedExtension(name="pgcrypto", schema="public", version="1.3")
    e_target = InspectedExtension(name="pgcrypto", schema="public", version=None)
    assert e_from != e_target
    stmts = e_target.alter_statements(e_from)
    for s in stmts:
        assert s is not None, f"BUG: alter_statements contains None: {stmts}"


# -- BUG 3: InspectedSelectable missing forcerowsecurity in __eq__ -----------


def test_forcerowsecurity_diff_detected():
    with temporary_database() as d1, temporary_database() as d2:
        with connect(d1) as s1, connect(d2) as s2:
            s1.execute("""
                create table t(id int primary key);
                alter table t enable row level security;
            """)
            s2.execute("""
                create table t(id int primary key);
                alter table t enable row level security;
                alter table t force row level security;
            """)
            i_from = get_inspector(s1)
            i_target = get_inspector(s2)

        t_from = i_from.relations['"public"."t"']
        t_target = i_target.relations['"public"."t"']
        assert t_from.forcerowsecurity != t_target.forcerowsecurity, (
            "Precondition: forcerowsecurity actually differs"
        )
        assert t_from != t_target, (
            "BUG: __eq__ ignores forcerowsecurity difference"
        )


# -- BUG 4: UserMapping __eq__ skips options when either is None -------------


def test_user_mapping_options_none_vs_some():
    m1 = InspectedUserMapping(server_name="srv", user_name="u1", options=None)
    m2 = InspectedUserMapping(
        server_name="srv", user_name="u1", options=["host=localhost"]
    )
    assert m1 != m2, (
        "BUG: user mappings with None vs some options compare as equal"
    )


def test_user_mapping_some_vs_none():
    m1 = InspectedUserMapping(
        server_name="srv", user_name="u1", options=["host=localhost"]
    )
    m2 = InspectedUserMapping(server_name="srv", user_name="u1", options=None)
    assert m1 != m2, (
        "BUG: user mappings with some vs None options compare as equal"
    )


# -- BUG 5: Publication owner not in __eq__ ---------------------------------


def test_publication_owner_change_detected():
    p1 = InspectedPublication(
        name="pub", publish_all_tables=True, publish_insert=True,
        publish_update=True, publish_delete=True, publish_truncate=True,
        publish_via_partition_root=False, owner="alice", tables=[],
    )
    p2 = InspectedPublication(
        name="pub", publish_all_tables=True, publish_insert=True,
        publish_update=True, publish_delete=True, publish_truncate=True,
        publish_via_partition_root=False, owner="bob", tables=[],
    )
    assert p1 != p2, (
        "BUG: publications with different owners compare as equal"
    )


# -- BUG 6: View owner-only change silently dropped -------------------------


def test_view_owner_only_change(pg_admin):
    """When only the owner changes on a view, diff should still detect it."""
    role1 = f"audit_role_{uuid4().hex[:8]}"
    role2 = f"audit_role_{uuid4().hex[:8]}"
    pg_admin.execute(f"create role {role1};")
    pg_admin.execute(f"create role {role2};")
    try:
        with temporary_database() as d1, temporary_database() as d2:
            with connect(d1) as s1, connect(d2) as s2:
                s1.execute("create view v as select 1 as x;")
                s1.execute(f"alter view v owner to {role1};")

                s2.execute("create view v as select 1 as x;")
                s2.execute(f"alter view v owner to {role2};")

                i_from = get_inspector(s1)
                i_target = get_inspector(s2)

            v_from = i_from.relations['"public"."v"']
            v_target = i_target.relations['"public"."v"']
            assert v_from.owner != v_target.owner, "Precondition: owners differ"

            changes = Changes(i_from, i_target)
            stmts = changes.selectables()
            sql = stmts.sql.lower()
            assert "owner to" in sql, (
                f"BUG: owner-only change on view produces no SQL. Got: {sql!r}"
            )
    finally:
        pg_admin.execute(f"drop role if exists {role1};")
        pg_admin.execute(f"drop role if exists {role2};")
