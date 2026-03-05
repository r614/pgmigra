from migra import Migration
from migra.db import connect, temporary_database


def test_modify_policy():
    """Verify modified RLS policies are handled as DROP+CREATE."""
    with (
        temporary_database(host="localhost") as d0,
        temporary_database(host="localhost") as d1,
    ):
        with connect(d0) as s0, connect(d1) as s1:
            s0.execute("""
                create table accounts(id int, owner text);
                alter table accounts enable row level security;
                create policy account_access on accounts
                    for select to public
                    using (owner = current_user);
            """)
            s1.execute("""
                create table accounts(id int, owner text);
                alter table accounts enable row level security;
                create policy account_access on accounts
                    for select to public
                    using (owner = current_user and id > 0);
            """)

        with connect(d0) as s0, connect(d1) as s1:
            m = Migration(s0, s1)
            m.inspect_from()
            m.inspect_target()
            m.set_safety(False)
            m.add_all_changes(privileges=True)
            sql = m.sql.lower()
            assert "drop policy" in sql
            assert "create policy" in sql
            assert "account_access" in sql


def test_rls_policy_equality_bug_fix():
    """Verify InspectedRowPolicy.__eq__ correctly compares self vs other."""
    with (
        temporary_database(host="localhost") as d0,
        temporary_database(host="localhost") as d1,
    ):
        with connect(d0) as s0, connect(d1) as s1:
            s0.execute("""
                create table t(id int, owner text);
                alter table t enable row level security;
                create policy p1 on t for select to public using (true);
            """)
            s1.execute("""
                create table t(id int, owner text);
                alter table t enable row level security;
                create policy p1 on t for select to public using (true);
            """)

        with connect(d0) as s0, connect(d1) as s1:
            m = Migration(s0, s1)
            m.inspect_from()
            m.inspect_target()
            m.set_safety(False)
            m.add_all_changes(privileges=True)
            assert m.sql.strip() == ""
