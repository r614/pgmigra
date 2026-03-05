from pgmigra import Migration
from pgmigra.command import parse_args
from pgmigra.db import connect, temporary_database


def test_concurrent_indexes():
    """Verify --concurrent-indexes rewrites CREATE INDEX to CREATE INDEX CONCURRENTLY."""
    with (
        temporary_database(host="localhost") as d0,
        temporary_database(host="localhost") as d1,
    ):
        with connect(d0) as s0, connect(d1) as s1:
            s0.execute("create table t(id int);")
            s1.execute("""
                create table t(id int);
                create index idx_t_id on t(id);
            """)

        with connect(d0) as s0, connect(d1) as s1:
            m = Migration(s0, s1)
            m.inspect_from()
            m.inspect_target()
            m.set_safety(False)
            m.add_all_changes(concurrent_indexes=True)
            sql = m.sql.lower()
            assert "create index concurrently" in sql
            assert "idx_t_id" in sql


def test_concurrent_indexes_off_by_default():
    """Verify CREATE INDEX is normal without --concurrent-indexes."""
    with (
        temporary_database(host="localhost") as d0,
        temporary_database(host="localhost") as d1,
    ):
        with connect(d0) as s0, connect(d1) as s1:
            s0.execute("create table t(id int);")
            s1.execute("""
                create table t(id int);
                create index idx_t_id on t(id);
            """)

        with connect(d0) as s0, connect(d1) as s1:
            m = Migration(s0, s1)
            m.inspect_from()
            m.inspect_target()
            m.set_safety(False)
            m.add_all_changes(concurrent_indexes=False)
            sql = m.sql.lower()
            assert "create index" in sql
            assert "concurrently" not in sql


def test_concurrent_indexes_cli_flag():
    """Verify --concurrent-indexes CLI flag is parsed correctly."""
    args = parse_args(["--concurrent-indexes", "--unsafe", "EMPTY", "EMPTY"])
    assert args.concurrent_indexes is True
    args = parse_args(["--unsafe", "EMPTY", "EMPTY"])
    assert args.concurrent_indexes is False
