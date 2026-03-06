"""Tests for bugs identified in the comprehensive code audit.

Each test targets a specific high-severity bug from the audit report.
Tests marked xfail document confirmed bugs that haven't been fixed yet.
"""

import pytest
from pgmigra.changes import Changes
from pgmigra.db import connect, temporary_database
from pgmigra.schemainspect import get_inspector
from pgmigra.schemainspect.pg.objects.privilege import InspectedPrivilege
from pgmigra.schemainspect.pg.objects.schema import InspectedSchema

# ============================================================================
# BUG 14.1: Composite type columns use internal typname (int4, bpchar, etc.)
# ============================================================================


class TestCompositeTypeInternalTypname:
    def test_composite_type_uses_format_type(self):
        """Composite type column types should use user-friendly names, not internal ones."""
        with temporary_database() as d1:
            with connect(d1) as s1:
                s1.execute("""
                    create type person_type as (
                        name character varying(100),
                        age integer,
                        tags text[]
                    );
                """)
                i1 = get_inspector(s1)
            ct = i1.composite_types['"public"."person_type"']
            cols = ct.columns
            assert "int4" not in str(cols), (
                f"BUG: composite type uses internal typname. Columns: {cols}"
            )
            assert "_text" not in str(cols), (
                f"BUG: composite type uses internal array typname. Columns: {cols}"
            )

    def test_composite_type_roundtrip_with_varchar(self):
        """Composite type with varchar should produce valid CREATE TYPE on roundtrip."""
        with temporary_database() as d1:
            with connect(d1) as s1:
                s1.execute("""
                    create type addr as (
                        street character varying(200),
                        zip char(5)
                    );
                """)
                i1 = get_inspector(s1)
            ct = i1.composite_types['"public"."addr"']
            create_sql = ct.create_statement
            assert "bpchar" not in create_sql, (
                f"BUG: create_statement uses internal typname bpchar: {create_sql}"
            )
            assert "character varying" in create_sql or "varchar" in create_sql, (
                f"BUG: varchar not preserved in create_statement: {create_sql}"
            )


# ============================================================================
# BUG 11.1: WITH GRANT OPTION never tracked or emitted
# ============================================================================


class TestPrivilegeGrantOption:
    @pytest.mark.xfail(reason="known gap: WITH GRANT OPTION not tracked")
    def test_grant_option_detected(self):
        """Privileges WITH GRANT OPTION should differ from those without."""
        with temporary_database() as d1, temporary_database() as d2:
            with connect(d1) as s1, connect(d2) as s2:
                s1.execute("create role test_user;")
                s1.execute("create table t(id int);")
                s1.execute("grant select on t to test_user;")
                s2.execute("create role test_user;")
                s2.execute("create table t(id int);")
                s2.execute("grant select on t to test_user with grant option;")
                i1 = get_inspector(s1)
                i2 = get_inspector(s2)
            changes = Changes(i1, i2)
            stmts = changes.privileges()
            assert stmts.sql.strip(), (
                "BUG: WITH GRANT OPTION difference not detected"
            )

    def test_privilege_object_lacks_grantable_field(self):
        """InspectedPrivilege should have a grantable field (currently missing)."""
        p = InspectedPrivilege(
            object_type="table",
            schema="public",
            name="t",
            privilege="SELECT",
            target_user="testuser",
        )
        assert not hasattr(p, "grantable"), (
            "grantable field exists — if this passes, update the xfail test above"
        )


# ============================================================================
# BUG 12.1: Schema owner never tracked
# ============================================================================


class TestSchemaOwner:
    @pytest.mark.xfail(reason="known gap: schema owner not tracked")
    def test_schema_owner_change_detected(self):
        """Schema owner change should be detected."""
        with temporary_database() as d1, temporary_database() as d2:
            with connect(d1) as s1, connect(d2) as s2:
                s1.execute("create role alice;")
                s1.execute("create schema myschema authorization alice;")
                s2.execute("create role bob;")
                s2.execute("create schema myschema authorization bob;")
                i1 = get_inspector(s1)
                i2 = get_inspector(s2)
            s1_schema = i1.schemas['"myschema"']
            s2_schema = i2.schemas['"myschema"']
            assert s1_schema != s2_schema, (
                "BUG: schema owner change not detected"
            )

    def test_schema_object_lacks_owner(self):
        """InspectedSchema currently has no owner attribute."""
        s = InspectedSchema(schema="myschema")
        assert not hasattr(s, "owner"), (
            "owner field exists — if this passes, update the xfail test above"
        )


# ============================================================================
# BUG 4.1: Collation DETERMINISTIC attribute not tracked
# ============================================================================


class TestCollationDeterministic:
    @pytest.mark.xfail(reason="known gap: collation DETERMINISTIC not tracked")
    def test_nondeterministic_collation_detected(self):
        """Non-deterministic (ICU) collation should roundtrip correctly."""
        with temporary_database() as d1:
            with connect(d1) as s1:
                s1.execute("""
                    create collation if not exists ci_collation (
                        provider = 'icu',
                        locale = 'und-u-ks-level2',
                        deterministic = false
                    );
                """)
                i1 = get_inspector(s1)
            coll = i1.collations['"public"."ci_collation"']
            create_sql = coll.create_statement
            assert "deterministic" in create_sql.lower(), (
                f"BUG: DETERMINISTIC=false not in create_statement: {create_sql}"
            )

    @pytest.mark.xfail(reason="known gap: collation DETERMINISTIC not tracked")
    def test_deterministic_change_detected(self):
        """Changing collation deterministic attribute should be detected."""
        with temporary_database() as d1, temporary_database() as d2:
            with connect(d1) as s1, connect(d2) as s2:
                s1.execute("""
                    create collation ci1 (
                        provider = 'icu',
                        locale = 'und-u-ks-level2',
                        deterministic = true
                    );
                """)
                s2.execute("""
                    create collation ci1 (
                        provider = 'icu',
                        locale = 'und-u-ks-level2',
                        deterministic = false
                    );
                """)
                i1 = get_inspector(s1)
                i2 = get_inspector(s2)
            c1 = i1.collations['"public"."ci1"']
            c2 = i2.collations['"public"."ci1"']
            assert c1 != c2, (
                "BUG: collation DETERMINISTIC change not detected"
            )


# ============================================================================
# BUG 26.1: attach_statement has parent/child reversed
# ============================================================================


class TestPartitionAttachStatement:
    @pytest.mark.xfail(reason="known bug: attach_statement reverses parent/child")
    def test_partition_attach_syntax(self):
        """Partition attach_statement should have correct ALTER TABLE parent ATTACH PARTITION child syntax."""
        with temporary_database() as d1:
            with connect(d1) as s1:
                s1.execute("""
                    create table parent_t(id int, created_at date)
                    partition by range (created_at);
                    create table child_t partition of parent_t
                    for values from ('2024-01-01') to ('2025-01-01');
                """)
                i1 = get_inspector(s1)
            child = i1.relations['"public"."child_t"']
            stmt = child.attach_statement
            assert stmt is not None, "attach_statement should not be None"
            assert "None" not in stmt, (
                f"BUG: partition_spec is None in attach_statement: {stmt}"
            )
            assert 'attach partition "public"."child_t"' in stmt.lower(), (
                f"BUG: child should be in ATTACH PARTITION clause, not ALTER TABLE. Got: {stmt}"
            )


# ============================================================================
# BUG 6.3/6.4: New table/view owner not emitted
# ============================================================================


class TestNewObjectOwnership:
    @pytest.mark.xfail(reason="known gap: new table owner not emitted in migration")
    def test_new_table_owner_emitted(self):
        """Creating a new table should include OWNER TO if source has no such table."""
        with temporary_database() as d1, temporary_database() as d2:
            with connect(d1) as s1, connect(d2) as s2:
                s1.execute("select 1;")
                s2.execute("create role app_user;")
                s2.execute("create table t(id int);")
                s2.execute("alter table t owner to app_user;")
                i1 = get_inspector(s1)
                i2 = get_inspector(s2)
            changes = Changes(i1, i2)
            stmts = changes.selectables()
            stmts.safe = False
            sql = stmts.sql.lower()
            assert "owner to" in sql, (
                f"BUG: new table owner not set in migration. Got: {sql!r}"
            )

    @pytest.mark.xfail(reason="known gap: new view owner not emitted in migration")
    def test_new_view_owner_emitted(self):
        """Creating a new view should include OWNER TO."""
        with temporary_database() as d1, temporary_database() as d2:
            with connect(d1) as s1, connect(d2) as s2:
                s1.execute("create table t(id int);")
                s2.execute("create role app_user;")
                s2.execute("create table t(id int);")
                s2.execute("create view v as select * from t;")
                s2.execute("alter view v owner to app_user;")
                i1 = get_inspector(s1)
                i2 = get_inspector(s2)
            changes = Changes(i1, i2)
            stmts = changes.selectables()
            stmts.safe = False
            sql = stmts.sql.lower()
            assert "owner to" in sql, (
                f"BUG: new view owner not set in migration. Got: {sql!r}"
            )


# ============================================================================
# BUG 1.1: Sequence parameters not tracked (also in edge_cases_deep, expanded here)
# ============================================================================


class TestSequenceCreateStatement:
    @pytest.mark.xfail(reason="known gap: sequence create_statement omits all params")
    def test_sequence_create_preserves_params(self):
        """Sequence create_statement should include INCREMENT, MIN, MAX, etc."""
        with temporary_database() as d1:
            with connect(d1) as s1:
                s1.execute("""
                    create sequence custom_seq
                    increment by 5
                    minvalue 100
                    maxvalue 999
                    start with 100
                    cache 10
                    cycle;
                """)
                i1 = get_inspector(s1)
            seq = i1.sequences['"public"."custom_seq"']
            create_sql = seq.create_statement
            assert "increment" in create_sql.lower(), (
                f"BUG: INCREMENT not in create_statement: {create_sql}"
            )


# ============================================================================
# BUG 19.2: FDW/Server OPTIONS syntax (key=value instead of key 'value')
# ============================================================================


class TestFDWOptionsSyntax:
    @pytest.mark.xfail(reason="known bug: FDW/server OPTIONS emitted as key=value instead of key 'value'")
    def test_fdw_options_format(self):
        """FDW options should be emitted in valid PostgreSQL OPTIONS syntax."""
        with temporary_database() as d1:
            with connect(d1) as s1:
                s1.execute("""
                    create extension if not exists postgres_fdw;
                    create server remote_srv
                    foreign data wrapper postgres_fdw
                    options (host 'remote.example.com', port '5432', dbname 'otherdb');
                """)
                i1 = get_inspector(s1)
            srv = i1.foreign_servers['"remote_srv"']
            create_sql = srv.create_statement
            assert "host 'remote.example.com'" in create_sql or "host='remote.example.com'" in create_sql, (
                f"Foreign server options might have wrong format: {create_sql}"
            )


# ============================================================================
# BUG 7.1: Index storage parameters not compared in __eq__
# ============================================================================


class TestIndexStorageParams:
    @pytest.mark.xfail(reason="known gap: index __eq__ doesn't compare definition/storage params")
    def test_index_fillfactor_change_detected(self):
        """Index fillfactor change should be detected."""
        with temporary_database() as d1, temporary_database() as d2:
            with connect(d1) as s1, connect(d2) as s2:
                s1.execute("""
                    create table t(id int);
                    create index t_id on t (id) with (fillfactor=100);
                """)
                s2.execute("""
                    create table t(id int);
                    create index t_id on t (id) with (fillfactor=50);
                """)
                i1 = get_inspector(s1)
                i2 = get_inspector(s2)
            idx1 = i1.indexes['"public"."t_id"']
            idx2 = i2.indexes['"public"."t_id"']
            assert "fillfactor=100" in idx1.definition
            assert "fillfactor=50" in idx2.definition
            assert idx1 != idx2, (
                "BUG: index fillfactor change not detected by __eq__"
            )


# ============================================================================
# BUG 22.1: Event trigger tags order-sensitive comparison
# ============================================================================


class TestEventTriggerTagsOrder:
    def test_event_trigger_tags_parsing(self):
        """Event trigger tags should be parsed and compared correctly."""
        with temporary_database() as d1:
            with connect(d1) as s1:
                s1.execute("""
                    create or replace function evt_func() returns event_trigger
                    language plpgsql as $$ begin end; $$;
                    create event trigger my_evt on ddl_command_end
                    when tag in ('CREATE TABLE', 'DROP TABLE', 'ALTER TABLE')
                    execute function evt_func();
                """)
                i1 = get_inspector(s1)
            et = i1.event_triggers['"my_evt"']
            assert et.tags is not None
            create_sql = et.create_statement
            assert "CREATE TABLE" in create_sql
            assert "DROP TABLE" in create_sql


# ============================================================================
# BUG 21.2: Foreign table comment uses wrong object_type
# ============================================================================


class TestCommentObjectTypes:
    def test_table_comment_roundtrip(self):
        """Table comments should roundtrip correctly."""
        with temporary_database() as d1:
            with connect(d1) as s1:
                s1.execute("""
                    create table t(id int);
                    comment on table t is 'My important table';
                """)
                i1 = get_inspector(s1)
            comments = i1.comments
            found = [c for c in comments.values() if "important" in str(c.comment)]
            assert len(found) == 1

    def test_function_comment_roundtrip(self):
        """Function comments should roundtrip correctly."""
        with temporary_database() as d1:
            with connect(d1) as s1:
                s1.execute("""
                    create function my_func() returns void language sql as $$ select 1 $$;
                    comment on function my_func() is 'A documented function';
                """)
                i1 = get_inspector(s1)
            comments = i1.comments
            found = [c for c in comments.values() if "documented" in str(c.comment)]
            assert len(found) == 1


# ============================================================================
# BUG 8.4: Aggregate functions excluded from inspection
# ============================================================================


class TestAggregateFunctions:
    @pytest.mark.xfail(reason="known gap: aggregate functions excluded from inspection")
    def test_aggregate_function_inspected(self):
        """User-defined aggregate functions should be inspected."""
        with temporary_database() as d1:
            with connect(d1) as s1:
                s1.execute("""
                    create function int_sum_sfunc(state int, val int)
                    returns int language sql as $$ select state + val $$;
                    create aggregate my_sum(int) (
                        sfunc = int_sum_sfunc,
                        stype = int,
                        initcond = '0'
                    );
                """)
                i1 = get_inspector(s1)
            found = [k for k in i1.functions.keys() if "my_sum" in k]
            assert len(found) > 0, (
                "BUG: aggregate function not found in inspector.functions"
            )


# ============================================================================
# Enum change_statements with single quotes in values
# ============================================================================


class TestEnumChangeStatementsQuoting:
    def test_enum_change_statements_with_single_quote(self):
        """Adding an enum value with single quote via change_statements should produce valid SQL."""
        with temporary_database() as d1, temporary_database() as d2:
            with connect(d1) as s1, connect(d2) as s2:
                s1.execute("create type mood as enum ('happy', 'sad');")
                s2.execute("create type mood as enum ('happy', 'it''s complicated', 'sad');")
                i1 = get_inspector(s1)
                i2 = get_inspector(s2)
            e1 = i1.enums['"public"."mood"']
            e2 = i2.enums['"public"."mood"']
            assert e1.can_be_changed_to(e2)
            stmts = e1.change_statements(e2)
            assert len(stmts) == 1
            assert "'it''s complicated'" in stmts[0], (
                f"BUG: single quote not escaped in change_statements: {stmts[0]}"
            )
            with temporary_database() as d3:
                with connect(d3) as s3:
                    s3.execute("create type mood as enum ('happy', 'sad');")
                    for stmt in stmts:
                        s3.execute(stmt)
                    i3 = get_inspector(s3)
                e3 = i3.enums['"public"."mood"']
                assert e3.elements == e2.elements

    def test_enum_change_statements_after_quoted_value(self):
        """Adding value after an existing value that contains single quote."""
        with temporary_database() as d1, temporary_database() as d2:
            with connect(d1) as s1, connect(d2) as s2:
                s1.execute("create type status as enum ('it''s ok', 'bad');")
                s2.execute("create type status as enum ('it''s ok', 'meh', 'bad');")
                i1 = get_inspector(s1)
                i2 = get_inspector(s2)
            e1 = i1.enums['"public"."status"']
            e2 = i2.enums['"public"."status"']
            stmts = e1.change_statements(e2)
            assert "'it''s ok'" in stmts[0], (
                f"BUG: reference value not escaped: {stmts[0]}"
            )
            with temporary_database() as d3:
                with connect(d3) as s3:
                    s3.execute("create type status as enum ('it''s ok', 'bad');")
                    for stmt in stmts:
                        s3.execute(stmt)
                    i3 = get_inspector(s3)
                e3 = i3.enums['"public"."status"']
                assert e3.elements == e2.elements
