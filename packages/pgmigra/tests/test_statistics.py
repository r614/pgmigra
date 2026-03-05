from pgmigra.changes import Changes
from pgmigra.db import connect, temporary_database
from pgmigra.schemainspect import get_inspector

SETUP = """
CREATE TABLE stats_test (
    id serial primary key,
    a int,
    b int,
    c text
);
"""


def test_statistics_ndistinct(db):
    with connect(db) as s:
        s.execute(SETUP)
        s.execute("CREATE STATISTICS st_ndistinct (ndistinct) ON a, b FROM stats_test;")
        i = get_inspector(s)

        key = '"public"."st_ndistinct"'
        assert key in i.statistics
        stat = i.statistics[key]
        assert stat.name == "st_ndistinct"
        assert stat.schema == "public"
        assert stat.table_name == "stats_test"
        assert "ndistinct" in stat.definition


def test_statistics_dependencies(db):
    with connect(db) as s:
        s.execute(SETUP)
        s.execute("CREATE STATISTICS st_deps (dependencies) ON a, b FROM stats_test;")
        i = get_inspector(s)

        key = '"public"."st_deps"'
        assert key in i.statistics
        stat = i.statistics[key]
        assert "dependencies" in stat.definition


def test_statistics_mcv(db):
    with connect(db) as s:
        s.execute(SETUP)
        s.execute("CREATE STATISTICS st_mcv (mcv) ON a, b FROM stats_test;")
        i = get_inspector(s)

        key = '"public"."st_mcv"'
        assert key in i.statistics
        stat = i.statistics[key]
        assert "mcv" in stat.definition


def test_statistics_expression(db):
    with connect(db) as s:
        s.execute(SETUP)
        s.execute("CREATE STATISTICS st_expr ON (a + b) FROM stats_test;")
        i = get_inspector(s)

        key = '"public"."st_expr"'
        assert key in i.statistics


def test_statistics_stattarget(db):
    with connect(db) as s:
        s.execute(SETUP)
        s.execute("CREATE STATISTICS st_target (ndistinct) ON a, b FROM stats_test;")
        s.execute("ALTER STATISTICS st_target SET STATISTICS 500;")
        i = get_inspector(s)

        key = '"public"."st_target"'
        stat = i.statistics[key]
        assert stat.stattarget == 500
        assert "SET STATISTICS 500" in stat.create_statement


def test_statistics_diff_add():
    with temporary_database() as d1, temporary_database() as d2:
        with connect(d1) as s1, connect(d2) as s2:
            s1.execute(SETUP)
            s2.execute(SETUP)
            s2.execute("CREATE STATISTICS st_new (ndistinct) ON a, b FROM stats_test;")
            i_from = get_inspector(s1)
            i_target = get_inspector(s2)

        changes = Changes(i_from, i_target)
        stmts = changes.statistics(creations_only=True)
        sql = stmts.sql
        assert "st_new" in sql
        assert "CREATE STATISTICS" in sql


def test_statistics_diff_drop():
    with temporary_database() as d1, temporary_database() as d2:
        with connect(d1) as s1, connect(d2) as s2:
            s1.execute(SETUP)
            s2.execute(SETUP)
            s1.execute("CREATE STATISTICS st_old (ndistinct) ON a, b FROM stats_test;")
            i_from = get_inspector(s1)
            i_target = get_inspector(s2)

        changes = Changes(i_from, i_target)
        stmts = changes.statistics(drops_only=True)
        stmts.safe = False
        sql = stmts.sql
        assert "DROP STATISTICS" in sql
        assert "st_old" in sql


def test_statistics_diff_modify():
    with temporary_database() as d1, temporary_database() as d2:
        with connect(d1) as s1, connect(d2) as s2:
            s1.execute(SETUP)
            s2.execute(SETUP)
            s1.execute("CREATE STATISTICS st_mod (ndistinct) ON a, b FROM stats_test;")
            s2.execute(
                "CREATE STATISTICS st_mod (ndistinct, dependencies) ON a, b FROM stats_test;"
            )
            i_from = get_inspector(s1)
            i_target = get_inspector(s2)

        changes = Changes(i_from, i_target)
        stmts = changes.statistics()
        stmts.safe = False
        sql = stmts.sql
        assert "DROP STATISTICS" in sql
        assert "CREATE STATISTICS" in sql


def test_statistics_roundtrip(db):
    with connect(db) as s:
        s.execute(SETUP)
        s.execute(
            "CREATE STATISTICS st_rt (ndistinct, dependencies) ON a, b FROM stats_test;"
        )
        i = get_inspector(s)
        key = '"public"."st_rt"'
        stat = i.statistics[key]
        create_sql = stat.create_statement
        drop_sql = stat.drop_statement

        s.execute(drop_sql)
        i2 = get_inspector(s)
        assert key not in i2.statistics

        s.execute(create_sql)
        i3 = get_inspector(s)
        stat3 = i3.statistics[key]
        assert stat == stat3


def test_statistics_default_stattarget(db):
    with connect(db) as s:
        s.execute(SETUP)
        s.execute("CREATE STATISTICS st_default (ndistinct) ON a, b FROM stats_test;")
        i = get_inspector(s)

        key = '"public"."st_default"'
        stat = i.statistics[key]
        assert stat.stattarget in (-1, None)
        assert "SET STATISTICS" not in stat.create_statement


def test_statistics_non_public_schema(db):
    with connect(db) as s:
        s.execute("CREATE SCHEMA other;")
        s.execute("CREATE TABLE other.t (a int, b int);")
        s.execute("CREATE STATISTICS other.st_other (ndistinct) ON a, b FROM other.t;")
        i = get_inspector(s)

        key = '"other"."st_other"'
        assert key in i.statistics
        stat = i.statistics[key]
        assert stat.schema == "other"
        assert stat.table_schema == "other"
        assert stat.table_name == "t"
        assert '"other"' in stat.drop_statement


def test_statistics_no_diff_when_equal():
    with temporary_database() as d1, temporary_database() as d2:
        with connect(d1) as s1, connect(d2) as s2:
            s1.execute(SETUP)
            s2.execute(SETUP)
            s1.execute("CREATE STATISTICS st_same (ndistinct) ON a, b FROM stats_test;")
            s2.execute("CREATE STATISTICS st_same (ndistinct) ON a, b FROM stats_test;")
            i_from = get_inspector(s1)
            i_target = get_inspector(s2)

        changes = Changes(i_from, i_target)
        stmts = changes.statistics()
        assert len(stmts) == 0


def test_statistics_stattarget_roundtrip(db):
    with connect(db) as s:
        s.execute(SETUP)
        s.execute("CREATE STATISTICS st_tgt (ndistinct) ON a, b FROM stats_test;")
        s.execute("ALTER STATISTICS st_tgt SET STATISTICS 200;")
        i = get_inspector(s)
        key = '"public"."st_tgt"'
        stat = i.statistics[key]
        assert stat.stattarget == 200

        create_sql = stat.create_statement
        drop_sql = stat.drop_statement

        s.execute(drop_sql)
        s.execute(create_sql)
        i2 = get_inspector(s)
        stat2 = i2.statistics[key]
        assert stat == stat2
        assert stat2.stattarget == 200


def test_statistics_combined_kinds(db):
    with connect(db) as s:
        s.execute(SETUP)
        s.execute(
            "CREATE STATISTICS st_combo (ndistinct, dependencies, mcv) ON a, b FROM stats_test;"
        )
        i = get_inspector(s)

        key = '"public"."st_combo"'
        stat = i.statistics[key]
        # When all three kinds are specified, PG may omit the kinds list
        # since it's equivalent to the default (all kinds)
        assert "st_combo" in stat.definition
        assert "a, b" in stat.definition
