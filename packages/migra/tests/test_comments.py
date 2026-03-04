from migra import Migration
from migra.db import connect, temporary_database


def test_comment_on_table():
    """Verify COMMENT ON is generated when table comments differ."""
    with (
        temporary_database(host="localhost") as d0,
        temporary_database(host="localhost") as d1,
    ):
        with connect(d0) as s0, connect(d1) as s1:
            s0.execute("create table users(id int);")
            s1.execute("""
                create table users(id int);
                comment on table users is 'User accounts table';
            """)

        with connect(d0) as s0, connect(d1) as s1:
            m = Migration(s0, s1)
            m.inspect_from()
            m.inspect_target()
            m.set_safety(False)
            m.add_all_changes()
            sql = m.sql.lower()
            assert "comment on" in sql
            assert "users" in sql


def test_comment_on_column():
    """Verify COMMENT ON COLUMN is generated for column comment changes."""
    with (
        temporary_database(host="localhost") as d0,
        temporary_database(host="localhost") as d1,
    ):
        with connect(d0) as s0, connect(d1) as s1:
            s0.execute("create table t(id int, name text);")
            s1.execute("""
                create table t(id int, name text);
                comment on column t.name is 'The display name';
            """)

        with connect(d0) as s0, connect(d1) as s1:
            m = Migration(s0, s1)
            m.inspect_from()
            m.inspect_target()
            m.set_safety(False)
            m.add_all_changes()
            sql = m.sql.lower()
            assert "comment on column" in sql
            assert "name" in sql


def test_comment_drop():
    """Verify COMMENT ON ... IS NULL is generated to remove a comment."""
    with (
        temporary_database(host="localhost") as d0,
        temporary_database(host="localhost") as d1,
    ):
        with connect(d0) as s0, connect(d1) as s1:
            s0.execute("""
                create table t(id int);
                comment on table t is 'Old comment';
            """)
            s1.execute("create table t(id int);")

        with connect(d0) as s0, connect(d1) as s1:
            m = Migration(s0, s1)
            m.inspect_from()
            m.inspect_target()
            m.set_safety(False)
            m.add_all_changes()
            sql = m.sql.lower()
            assert "comment on" in sql
            assert "is null" in sql
