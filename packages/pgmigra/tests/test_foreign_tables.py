from pgmigra.changes import Changes
from pgmigra.db import connect, temporary_database
from pgmigra.schemainspect import get_inspector

SETUP = """
CREATE EXTENSION IF NOT EXISTS file_fdw;
CREATE SERVER test_srv FOREIGN DATA WRAPPER file_fdw;
"""

FT_OPTIONS = "OPTIONS (filename '/dev/null')"


def test_foreign_table_inspect(db):
    with connect(db) as s:
        s.execute(SETUP)
        s.execute(
            f"CREATE FOREIGN TABLE ft_test (id int, name text) SERVER test_srv {FT_OPTIONS};"
        )
        i = get_inspector(s)

        key = '"public"."ft_test"'
        assert key in i.foreign_tables
        assert key in i.relations

        ft = i.foreign_tables[key]
        assert ft.relationtype == "ft"
        assert ft.ft_server_name == "test_srv"
        assert "id" in ft.columns
        assert "name" in ft.columns


def test_foreign_table_create_statement(db):
    with connect(db) as s:
        s.execute(SETUP)
        s.execute(
            f"CREATE FOREIGN TABLE ft_cs (id int, val text) SERVER test_srv {FT_OPTIONS};"
        )
        i = get_inspector(s)

        ft = i.foreign_tables['"public"."ft_cs"']
        stmt = ft.create_statement
        assert "create foreign table" in stmt
        assert "ft_cs" in stmt
        assert "server test_srv" in stmt


def test_foreign_table_drop_statement(db):
    with connect(db) as s:
        s.execute(SETUP)
        s.execute(
            f"CREATE FOREIGN TABLE ft_drop (id int) SERVER test_srv {FT_OPTIONS};"
        )
        i = get_inspector(s)

        ft = i.foreign_tables['"public"."ft_drop"']
        stmt = ft.drop_statement
        assert "drop foreign table" in stmt
        assert "ft_drop" in stmt


def test_foreign_table_is_not_regular_table(db):
    with connect(db) as s:
        s.execute(SETUP)
        s.execute(
            f"CREATE FOREIGN TABLE ft_kind (id int) SERVER test_srv {FT_OPTIONS};"
        )
        i = get_inspector(s)

        ft = i.foreign_tables['"public"."ft_kind"']
        assert ft.is_table is False
        assert ft.relationtype == "ft"


def test_foreign_table_diff_add():
    with temporary_database() as d1, temporary_database() as d2:
        with connect(d1) as s1, connect(d2) as s2:
            s1.execute(SETUP)
            s2.execute(SETUP)
            s2.execute(
                f"CREATE FOREIGN TABLE ft_new (id int, val text) SERVER test_srv {FT_OPTIONS};"
            )
            i_from = get_inspector(s1)
            i_target = get_inspector(s2)

        changes = Changes(i_from, i_target)
        stmts = changes.non_table_selectable_creations()
        sql = stmts.sql
        assert "create foreign table" in sql
        assert "ft_new" in sql


def test_foreign_table_diff_drop():
    with temporary_database() as d1, temporary_database() as d2:
        with connect(d1) as s1, connect(d2) as s2:
            s1.execute(SETUP)
            s2.execute(SETUP)
            s1.execute(
                f"CREATE FOREIGN TABLE ft_old (id int, val text) SERVER test_srv {FT_OPTIONS};"
            )
            i_from = get_inspector(s1)
            i_target = get_inspector(s2)

        changes = Changes(i_from, i_target)
        stmts = changes.non_table_selectable_drops()
        stmts.safe = False
        sql = stmts.sql
        assert "drop foreign table" in sql
        assert "ft_old" in sql


def test_foreign_table_no_diff_when_equal():
    with temporary_database() as d1, temporary_database() as d2:
        with connect(d1) as s1, connect(d2) as s2:
            s1.execute(SETUP)
            s2.execute(SETUP)
            s1.execute(
                f"CREATE FOREIGN TABLE ft_same (id int, val text) SERVER test_srv {FT_OPTIONS};"
            )
            s2.execute(
                f"CREATE FOREIGN TABLE ft_same (id int, val text) SERVER test_srv {FT_OPTIONS};"
            )
            i_from = get_inspector(s1)
            i_target = get_inspector(s2)

        changes = Changes(i_from, i_target)
        drops = changes.non_table_selectable_drops()
        creates = changes.non_table_selectable_creations()
        assert len(drops) == 0
        assert len(creates) == 0


def test_foreign_table_in_deps(db):
    with connect(db) as s:
        s.execute(SETUP)
        s.execute(f"CREATE FOREIGN TABLE ft_dep (id int) SERVER test_srv {FT_OPTIONS};")
        s.execute("CREATE VIEW v_dep AS SELECT * FROM ft_dep;")
        i = get_inspector(s)

        view = i.views['"public"."v_dep"']
        assert '"public"."ft_dep"' in view.dependent_on


def test_foreign_table_roundtrip(db):
    with connect(db) as s:
        s.execute(SETUP)
        s.execute(
            f"CREATE FOREIGN TABLE ft_rt (id int, name text) SERVER test_srv {FT_OPTIONS};"
        )
        i = get_inspector(s)
        ft = i.foreign_tables['"public"."ft_rt"']
        drop_sql = ft.drop_statement
        create_sql = ft.create_statement

        s.execute(drop_sql)
        i2 = get_inspector(s)
        assert '"public"."ft_rt"' not in i2.foreign_tables

        s.execute(create_sql)
        i3 = get_inspector(s)
        ft3 = i3.foreign_tables['"public"."ft_rt"']
        assert ft == ft3
