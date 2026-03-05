from pgmigra.changes import Changes
from pgmigra.db import connect, temporary_database
from pgmigra.schemainspect import get_inspector

SETUP_TABLES = """
CREATE TABLE t1 (id int primary key, val text);
CREATE TABLE t2 (id int primary key, val text);
CREATE TABLE t3 (id int primary key, val text);
"""


def test_publication_all_tables(db):
    with connect(db) as s:
        s.execute(SETUP_TABLES)
        s.execute("CREATE PUBLICATION pub_all FOR ALL TABLES;")
        i = get_inspector(s)

        pub = i.publications['"pub_all"']
        assert pub.name == "pub_all"
        assert pub.publish_all_tables is True
        assert pub.publish_insert is True
        assert pub.publish_update is True
        assert pub.publish_delete is True
        assert pub.publish_truncate is True
        assert "FOR ALL TABLES" in pub.create_statement


def test_publication_specific_tables(db):
    with connect(db) as s:
        s.execute(SETUP_TABLES)
        s.execute("CREATE PUBLICATION pub_specific FOR TABLE t1, t2;")
        i = get_inspector(s)

        pub = i.publications['"pub_specific"']
        assert pub.name == "pub_specific"
        assert pub.publish_all_tables is False
        assert len(pub.tables) == 2
        assert "public.t1" in pub.tables
        assert "public.t2" in pub.tables


def test_publication_with_options(db):
    with connect(db) as s:
        s.execute(SETUP_TABLES)
        s.execute(
            "CREATE PUBLICATION pub_opts FOR TABLE t1 WITH (publish = 'insert, update');"
        )
        i = get_inspector(s)

        pub = i.publications['"pub_opts"']
        assert pub.publish_insert is True
        assert pub.publish_update is True
        assert pub.publish_delete is False
        assert pub.publish_truncate is False
        assert "publish = 'insert, update'" in pub.create_statement


def test_publication_diff_add():
    with temporary_database() as d1, temporary_database() as d2:
        with connect(d1) as s1, connect(d2) as s2:
            s1.execute(SETUP_TABLES)
            s2.execute(SETUP_TABLES)
            s2.execute("CREATE PUBLICATION pub_new FOR ALL TABLES;")
            i_from = get_inspector(s1)
            i_target = get_inspector(s2)

        changes = Changes(i_from, i_target)
        stmts = changes.publications(creations_only=True)
        sql = stmts.sql
        assert "CREATE PUBLICATION" in sql
        assert "pub_new" in sql


def test_publication_diff_drop():
    with temporary_database() as d1, temporary_database() as d2:
        with connect(d1) as s1, connect(d2) as s2:
            s1.execute(SETUP_TABLES)
            s2.execute(SETUP_TABLES)
            s1.execute("CREATE PUBLICATION pub_old FOR ALL TABLES;")
            i_from = get_inspector(s1)
            i_target = get_inspector(s2)

        changes = Changes(i_from, i_target)
        stmts = changes.publications(drops_only=True)
        stmts.safe = False
        sql = stmts.sql
        assert "DROP PUBLICATION" in sql
        assert "pub_old" in sql


def test_publication_diff_modify_options():
    with temporary_database() as d1, temporary_database() as d2:
        with connect(d1) as s1, connect(d2) as s2:
            s1.execute(SETUP_TABLES)
            s2.execute(SETUP_TABLES)
            s1.execute("CREATE PUBLICATION pub_mod FOR ALL TABLES;")
            s2.execute(
                "CREATE PUBLICATION pub_mod FOR ALL TABLES WITH (publish = 'insert, update');"
            )
            i_from = get_inspector(s1)
            i_target = get_inspector(s2)

        changes = Changes(i_from, i_target)
        stmts = changes.publications()
        sql = stmts.sql
        assert "ALTER PUBLICATION" in sql


def test_publication_diff_modify_tables():
    with temporary_database() as d1, temporary_database() as d2:
        with connect(d1) as s1, connect(d2) as s2:
            s1.execute(SETUP_TABLES)
            s2.execute(SETUP_TABLES)
            s1.execute("CREATE PUBLICATION pub_tables FOR TABLE t1;")
            s2.execute("CREATE PUBLICATION pub_tables FOR TABLE t1, t2;")
            i_from = get_inspector(s1)
            i_target = get_inspector(s2)

        changes = Changes(i_from, i_target)
        stmts = changes.publications()
        sql = stmts.sql
        assert "ALTER PUBLICATION" in sql
        assert "SET TABLE" in sql


def test_publication_roundtrip(db):
    with connect(db) as s:
        s.execute(SETUP_TABLES)
        s.execute(
            "CREATE PUBLICATION pub_rt FOR TABLE t1, t2 WITH (publish = 'insert, delete');"
        )
        i = get_inspector(s)
        pub = i.publications['"pub_rt"']
        create_sql = pub.create_statement
        drop_sql = pub.drop_statement

        s.execute(drop_sql)
        i2 = get_inspector(s)
        assert '"pub_rt"' not in i2.publications

        s.execute(create_sql)
        i3 = get_inspector(s)
        pub3 = i3.publications['"pub_rt"']
        assert pub == pub3


def test_publication_no_tables(db):
    with connect(db) as s:
        s.execute("CREATE PUBLICATION pub_empty;")
        i = get_inspector(s)

        pub = i.publications['"pub_empty"']
        assert pub.publish_all_tables is False
        assert pub.tables == []
        assert "FOR ALL TABLES" not in pub.create_statement
        assert "FOR TABLE" not in pub.create_statement


def test_publication_via_partition_root(db):
    with connect(db) as s:
        s.execute(SETUP_TABLES)
        s.execute(
            "CREATE PUBLICATION pub_vpr FOR ALL TABLES WITH (publish_via_partition_root = true);"
        )
        i = get_inspector(s)

        pub = i.publications['"pub_vpr"']
        assert pub.publish_via_partition_root is True
        assert "publish_via_partition_root" in pub.create_statement


def test_publication_no_diff_when_equal():
    with temporary_database() as d1, temporary_database() as d2:
        with connect(d1) as s1, connect(d2) as s2:
            s1.execute(SETUP_TABLES)
            s2.execute(SETUP_TABLES)
            s1.execute("CREATE PUBLICATION pub_same FOR TABLE t1, t2;")
            s2.execute("CREATE PUBLICATION pub_same FOR TABLE t1, t2;")
            i_from = get_inspector(s1)
            i_target = get_inspector(s2)

        changes = Changes(i_from, i_target)
        stmts = changes.publications()
        assert len(stmts) == 0


def test_publication_quoted_name(db):
    with connect(db) as s:
        s.execute(SETUP_TABLES)
        s.execute('CREATE PUBLICATION "My-Pub" FOR ALL TABLES;')
        i = get_inspector(s)

        assert '"My-Pub"' in i.publications
        pub = i.publications['"My-Pub"']
        assert pub.name == "My-Pub"
        assert '"My-Pub"' in pub.create_statement
        assert '"My-Pub"' in pub.drop_statement


def test_publication_diff_remove_all_tables():
    with temporary_database() as d1, temporary_database() as d2:
        with connect(d1) as s1, connect(d2) as s2:
            s1.execute(SETUP_TABLES)
            s2.execute(SETUP_TABLES)
            s1.execute("CREATE PUBLICATION pub_shrink FOR TABLE t1, t2;")
            s2.execute("CREATE PUBLICATION pub_shrink FOR TABLE t1;")
            i_from = get_inspector(s1)
            i_target = get_inspector(s2)

        changes = Changes(i_from, i_target)
        stmts = changes.publications()
        sql = stmts.sql
        assert "ALTER PUBLICATION" in sql
        assert "SET TABLE" in sql
