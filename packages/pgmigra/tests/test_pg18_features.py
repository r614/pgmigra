import pytest
from pgmigra.changes import Changes
from pgmigra.db import connect, temporary_database
from pgmigra.schemainspect import get_inspector

pytestmark = pytest.mark.requires_pg(min_version=18)


# -- Virtual generated columns ------------------------------------------------


def test_virtual_generated_column_inspect(db):
    with connect(db) as s:
        s.execute("""
            create table t(
                a int,
                b int generated always as (a * 2) virtual
            )
        """)
        i = get_inspector(s)
        cols = i.relations['"public"."t"'].columns
        assert cols["b"].is_generated is True
        assert cols["b"].generated_type == "v"
        assert "virtual" in cols["b"].creation_clause.lower()


def test_virtual_generated_column_diff():
    with temporary_database() as d1, temporary_database() as d2:
        with connect(d1) as s1, connect(d2) as s2:
            s1.execute("create table t(a int);")
            s2.execute("""
                create table t(
                    a int,
                    b int generated always as (a * 2) virtual
                )
            """)
            i_from = get_inspector(s1)
            i_target = get_inspector(s2)

        changes = Changes(i_from, i_target)
        stmts = changes.selectables(creations_only=True)
        stmts.safe = False
        sql = stmts.sql.lower()
        assert "generated always as" in sql
        assert "virtual" in sql


# -- NOT ENFORCED constraints ------------------------------------------------


def test_not_enforced_check_constraint(db):
    with connect(db) as s:
        s.execute("""
            create table t(
                a int,
                constraint chk_a check (a > 0) not enforced
            )
        """)
        i = get_inspector(s)
        t = i.relations['"public"."t"']
        constraints = list(t.constraints.values())
        assert len(constraints) == 1
        assert "not enforced" in constraints[0].definition.lower()


def test_not_enforced_fk_constraint(db):
    with connect(db) as s:
        s.execute("""
            create table parent(id int primary key);
            create table child(
                id int,
                parent_id int,
                constraint fk_parent foreign key (parent_id)
                    references parent(id) not enforced
            );
        """)
        i = get_inspector(s)
        t = i.relations['"public"."child"']
        fk = [c for c in t.constraints.values() if c.is_fk]
        assert len(fk) == 1
        assert "not enforced" in fk[0].definition.lower()


def test_not_enforced_constraint_diff():
    with temporary_database() as d1, temporary_database() as d2:
        with connect(d1) as s1, connect(d2) as s2:
            s1.execute("create table t(a int);")
            s2.execute("""
                create table t(
                    a int,
                    constraint chk_a check (a > 0) not enforced
                )
            """)
            i_from = get_inspector(s1)
            i_target = get_inspector(s2)

        changes = Changes(i_from, i_target)
        stmts = changes.constraints(creations_only=True)
        sql = stmts.sql.lower()
        assert "not enforced" in sql


# -- Temporal constraints (WITHOUT OVERLAPS) ---------------------------------


def test_temporal_pk_without_overlaps(db):
    with connect(db) as s:
        s.execute("create extension if not exists btree_gist;")
        s.execute("""
            create table reservations(
                id int,
                valid_range tstzrange,
                primary key (id, valid_range without overlaps)
            )
        """)
        i = get_inspector(s)
        t = i.relations['"public"."reservations"']
        pk = [c for c in t.constraints.values() if c.constraint_type == "PRIMARY KEY"]
        assert len(pk) == 1
        assert "without overlaps" in pk[0].definition.lower()


def test_temporal_fk_period(db):
    with connect(db) as s:
        s.execute("create extension if not exists btree_gist;")
        s.execute("""
            create table parent(
                id int,
                valid_range tstzrange,
                primary key (id, valid_range without overlaps)
            );
            create table child(
                id int,
                parent_id int,
                valid_range tstzrange,
                foreign key (parent_id, period valid_range)
                    references parent(id, period valid_range)
            );
        """)
        i = get_inspector(s)
        t = i.relations['"public"."child"']
        fk = [c for c in t.constraints.values() if c.is_fk]
        assert len(fk) == 1
        assert "period" in fk[0].definition.lower()


# -- publish_generated_columns publication option ----------------------------


SETUP_TABLES = """
create table t1 (id int primary key, val text);
"""


def test_publish_generated_columns_inspect(db):
    with connect(db) as s:
        s.execute(SETUP_TABLES)
        s.execute(
            "create publication pub_gc for all tables"
            " with (publish_generated_columns = 'stored');"
        )
        i = get_inspector(s)
        pub = i.publications['"pub_gc"']
        assert pub.publish_generated_columns == "stored"
        assert "publish_generated_columns" in pub.create_statement


def test_publish_generated_columns_diff():
    with temporary_database() as d1, temporary_database() as d2:
        with connect(d1) as s1, connect(d2) as s2:
            s1.execute(SETUP_TABLES)
            s2.execute(SETUP_TABLES)
            s1.execute("create publication pub_gc for all tables;")
            s2.execute(
                "create publication pub_gc for all tables"
                " with (publish_generated_columns = 'stored');"
            )
            i_from = get_inspector(s1)
            i_target = get_inspector(s2)

        changes = Changes(i_from, i_target)
        stmts = changes.publications()
        sql = stmts.sql
        assert "ALTER PUBLICATION" in sql
        assert "publish_generated_columns" in sql


def test_publish_generated_columns_diff_reset():
    with temporary_database() as d1, temporary_database() as d2:
        with connect(d1) as s1, connect(d2) as s2:
            s1.execute(SETUP_TABLES)
            s2.execute(SETUP_TABLES)
            s1.execute(
                "create publication pub_gc for all tables"
                " with (publish_generated_columns = 'stored');"
            )
            s2.execute("create publication pub_gc for all tables;")
            i_from = get_inspector(s1)
            i_target = get_inspector(s2)

        changes = Changes(i_from, i_target)
        stmts = changes.publications()
        sql = stmts.sql
        assert "ALTER PUBLICATION" in sql
        assert "publish_generated_columns = 'none'" in sql


def test_publish_generated_columns_roundtrip(db):
    with connect(db) as s:
        s.execute(SETUP_TABLES)
        s.execute(
            "create publication pub_gc for all tables"
            " with (publish_generated_columns = 'stored');"
        )
        i = get_inspector(s)
        pub = i.publications['"pub_gc"']
        create_sql = pub.create_statement
        drop_sql = pub.drop_statement

        s.execute(drop_sql)
        s.execute(create_sql)
        i2 = get_inspector(s)
        pub2 = i2.publications['"pub_gc"']
        assert pub == pub2
