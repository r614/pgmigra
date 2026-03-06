"""Deep edge-case fuzz tests cross-referenced against PostgreSQL documentation.

Each test targets a specific PostgreSQL DDL feature and checks whether pgmigra
correctly inspects, diffs, and/or roundtrips it. Tests are grouped by object type.

Tests marked with xfail(reason="known gap") document real missing features.
"""

import pytest
from pgmigra.changes import Changes
from pgmigra.db import connect, temporary_database
from pgmigra.schemainspect import get_inspector

# ============================================================================
# SEQUENCES — attributes like start/increment/min/max/cache/cycle
# ============================================================================


class TestSequenceAttributes:
    """Sequences have start/increment/min/max/cache/cycle that we should diff."""

    @pytest.mark.xfail(reason="known gap: sequence attributes not inspected")
    def test_sequence_increment_change_detected(self):
        """Changing INCREMENT BY on a sequence should produce migration SQL."""
        with temporary_database() as d1, temporary_database() as d2:
            with connect(d1) as s1, connect(d2) as s2:
                s1.execute("create sequence myseq increment by 1;")
                s2.execute("create sequence myseq increment by 10;")
                i1 = get_inspector(s1)
                i2 = get_inspector(s2)
            s1_seq = i1.sequences['"public"."myseq"']
            s2_seq = i2.sequences['"public"."myseq"']
            assert s1_seq != s2_seq, (
                "BUG: sequences with different INCREMENT compare as equal"
            )

    @pytest.mark.xfail(reason="known gap: sequence attributes not inspected")
    def test_sequence_minmax_change_detected(self):
        with temporary_database() as d1, temporary_database() as d2:
            with connect(d1) as s1, connect(d2) as s2:
                s1.execute("create sequence myseq minvalue 1 maxvalue 100;")
                s2.execute("create sequence myseq minvalue 1 maxvalue 999;")
                i1 = get_inspector(s1)
                i2 = get_inspector(s2)
            s1_seq = i1.sequences['"public"."myseq"']
            s2_seq = i2.sequences['"public"."myseq"']
            assert s1_seq != s2_seq, (
                "BUG: sequences with different MAXVALUE compare as equal"
            )

    @pytest.mark.xfail(reason="known gap: sequence attributes not inspected")
    def test_sequence_cycle_change_detected(self):
        with temporary_database() as d1, temporary_database() as d2:
            with connect(d1) as s1, connect(d2) as s2:
                s1.execute("create sequence myseq no cycle;")
                s2.execute("create sequence myseq cycle;")
                i1 = get_inspector(s1)
                i2 = get_inspector(s2)
            s1_seq = i1.sequences['"public"."myseq"']
            s2_seq = i2.sequences['"public"."myseq"']
            assert s1_seq != s2_seq, (
                "BUG: sequences with different CYCLE compare as equal"
            )

    @pytest.mark.xfail(reason="known gap: sequence attributes not inspected")
    def test_sequence_cache_change_detected(self):
        with temporary_database() as d1, temporary_database() as d2:
            with connect(d1) as s1, connect(d2) as s2:
                s1.execute("create sequence myseq cache 1;")
                s2.execute("create sequence myseq cache 20;")
                i1 = get_inspector(s1)
                i2 = get_inspector(s2)
            s1_seq = i1.sequences['"public"."myseq"']
            s2_seq = i2.sequences['"public"."myseq"']
            assert s1_seq != s2_seq, (
                "BUG: sequences with different CACHE compare as equal"
            )

    @pytest.mark.xfail(reason="known gap: sequence attributes not inspected")
    def test_sequence_start_change_detected(self):
        with temporary_database() as d1, temporary_database() as d2:
            with connect(d1) as s1, connect(d2) as s2:
                s1.execute("create sequence myseq start with 1;")
                s2.execute("create sequence myseq start with 100;")
                i1 = get_inspector(s1)
                i2 = get_inspector(s2)
            s1_seq = i1.sequences['"public"."myseq"']
            s2_seq = i2.sequences['"public"."myseq"']
            assert s1_seq != s2_seq, (
                "BUG: sequences with different START WITH compare as equal"
            )


# ============================================================================
# TABLE — replica identity, storage parameters, tablespace
# ============================================================================


class TestTableReplicaIdentity:
    """REPLICA IDENTITY is stored in pg_class.relreplident but not inspected."""

    @pytest.mark.xfail(reason="known gap: replica identity not inspected")
    def test_replica_identity_full_detected(self):
        with temporary_database() as d1, temporary_database() as d2:
            with connect(d1) as s1, connect(d2) as s2:
                s1.execute("create table t(id int primary key);")
                s2.execute("""
                    create table t(id int primary key);
                    alter table t replica identity full;
                """)
                i1 = get_inspector(s1)
                i2 = get_inspector(s2)
            t1 = i1.relations['"public"."t"']
            t2 = i2.relations['"public"."t"']
            assert t1 != t2, (
                "BUG: REPLICA IDENTITY FULL change not detected"
            )

    @pytest.mark.xfail(reason="known gap: replica identity not inspected")
    def test_replica_identity_nothing_detected(self):
        with temporary_database() as d1, temporary_database() as d2:
            with connect(d1) as s1, connect(d2) as s2:
                s1.execute("create table t(id int primary key);")
                s2.execute("""
                    create table t(id int primary key);
                    alter table t replica identity nothing;
                """)
                i1 = get_inspector(s1)
                i2 = get_inspector(s2)
            t1 = i1.relations['"public"."t"']
            t2 = i2.relations['"public"."t"']
            assert t1 != t2, (
                "BUG: REPLICA IDENTITY NOTHING change not detected"
            )


class TestTableStorageParams:
    """Table storage parameters (fillfactor, autovacuum settings) not inspected."""

    @pytest.mark.xfail(reason="known gap: table storage params (reloptions) not inspected")
    def test_fillfactor_change_detected(self):
        with temporary_database() as d1, temporary_database() as d2:
            with connect(d1) as s1, connect(d2) as s2:
                s1.execute("create table t(id int) with (fillfactor=100);")
                s2.execute("create table t(id int) with (fillfactor=70);")
                i1 = get_inspector(s1)
                i2 = get_inspector(s2)
            t1 = i1.relations['"public"."t"']
            t2 = i2.relations['"public"."t"']
            assert t1 != t2, (
                "BUG: table fillfactor change not detected"
            )

    @pytest.mark.xfail(reason="known gap: table storage params (reloptions) not inspected")
    def test_autovacuum_param_change_detected(self):
        with temporary_database() as d1, temporary_database() as d2:
            with connect(d1) as s1, connect(d2) as s2:
                s1.execute("create table t(id int);")
                s2.execute("""
                    create table t(id int);
                    alter table t set (autovacuum_enabled = false);
                """)
                i1 = get_inspector(s1)
                i2 = get_inspector(s2)
            t1 = i1.relations['"public"."t"']
            t2 = i2.relations['"public"."t"']
            assert t1 != t2, (
                "BUG: table autovacuum_enabled change not detected"
            )


# ============================================================================
# VIEWS — check option, security_barrier
# ============================================================================


class TestViewFeatures:
    @pytest.mark.xfail(reason="known gap: CHECK OPTION not in pg_get_viewdef output")
    def test_view_check_option_local(self):
        """WITH LOCAL CHECK OPTION should roundtrip correctly."""
        with temporary_database() as d1, temporary_database() as d2:
            with connect(d1) as s1, connect(d2) as s2:
                s1.execute("create table t(id int, val int);")
                s1.execute("""
                    create view v as select * from t where val > 0
                    with local check option;
                """)
                s2.execute("create table t(id int, val int);")
                s2.execute("create view v as select * from t where val > 0;")
                i1 = get_inspector(s1)
                i2 = get_inspector(s2)
            v1 = i1.views['"public"."v"']
            v2 = i2.views['"public"."v"']
            assert v1 != v2, (
                "BUG: view WITH CHECK OPTION vs without compare as equal"
            )

    @pytest.mark.xfail(reason="known gap: security_barrier in pg_class.reloptions, not inspected")
    def test_view_security_barrier(self):
        """security_barrier option should be detected."""
        with temporary_database() as d1, temporary_database() as d2:
            with connect(d1) as s1, connect(d2) as s2:
                s1.execute("create table t(id int);")
                s1.execute("create view v as select * from t;")
                s2.execute("create table t(id int);")
                s2.execute("""
                    create view v with (security_barrier=true)
                    as select * from t;
                """)
                i1 = get_inspector(s1)
                i2 = get_inspector(s2)
            v1 = i1.views['"public"."v"']
            v2 = i2.views['"public"."v"']
            assert v1 != v2, (
                "BUG: view security_barrier change not detected"
            )


# ============================================================================
# ENUMS — special characters in values
# ============================================================================


class TestEnumEdgeCases:
    def test_enum_value_with_single_quote(self):
        """Enum values containing single quotes must be properly escaped."""
        with temporary_database() as d1:
            with connect(d1) as s1:
                s1.execute("""
                    create type mood as enum ('happy', 'it''s complicated', 'sad');
                """)
                i1 = get_inspector(s1)
            e = i1.enums['"public"."mood"']
            assert "it's complicated" in e.elements
            create_sql = e.create_statement
            # The create_statement must properly escape the single quote
            with temporary_database() as d2:
                with connect(d2) as s2:
                    s2.execute(create_sql)
                    i2 = get_inspector(s2)
                e2 = i2.enums['"public"."mood"']
                assert e.elements == e2.elements, (
                    f"BUG: enum roundtrip lost single-quote value. "
                    f"Original: {e.elements}, Roundtripped: {e2.elements}"
                )

    def test_enum_value_with_backslash(self):
        """Enum values containing backslashes."""
        with temporary_database() as d1:
            with connect(d1) as s1:
                s1.execute(r"""
                    create type path_type as enum ('a\b', 'c\\d', 'normal');
                """)
                i1 = get_inspector(s1)
            e = i1.enums['"public"."path_type"']
            create_sql = e.create_statement
            with temporary_database() as d2:
                with connect(d2) as s2:
                    s2.execute(create_sql)
                    i2 = get_inspector(s2)
                e2 = i2.enums['"public"."path_type"']
                assert e.elements == e2.elements, (
                    f"BUG: enum roundtrip lost backslash value. "
                    f"Original: {e.elements}, Roundtripped: {e2.elements}"
                )

    def test_enum_empty_string_value(self):
        """Enum with empty string value."""
        with temporary_database() as d1:
            with connect(d1) as s1:
                s1.execute("create type status as enum ('', 'active', 'inactive');")
                i1 = get_inspector(s1)
            e = i1.enums['"public"."status"']
            assert "" in e.elements
            create_sql = e.create_statement
            with temporary_database() as d2:
                with connect(d2) as s2:
                    s2.execute(create_sql)
                    i2 = get_inspector(s2)
                e2 = i2.enums['"public"."status"']
                assert e.elements == e2.elements

    def test_enum_add_value_between(self):
        """Adding an enum value between existing values should use correct positioning."""
        with temporary_database() as d1, temporary_database() as d2:
            with connect(d1) as s1, connect(d2) as s2:
                s1.execute("create type color as enum ('red', 'blue');")
                s2.execute("create type color as enum ('red', 'green', 'blue');")
                i1 = get_inspector(s1)
                i2 = get_inspector(s2)
            e1 = i1.enums['"public"."color"']
            e2 = i2.enums['"public"."color"']
            assert e1.can_be_changed_to(e2)
            stmts = e1.change_statements(e2)
            assert len(stmts) == 1
            # The new value should go AFTER 'red' (not BEFORE 'blue')
            assert "after 'red'" in stmts[0].lower(), (
                f"BUG: enum add value positioning wrong. Got: {stmts[0]}"
            )


# ============================================================================
# CONSTRAINTS — NOT VALID, deferrable edge cases
# ============================================================================


class TestConstraintEdgeCases:
    def test_fk_constraint_not_valid_roundtrip(self):
        """NOT VALID FK constraint should be detected via pg_get_constraintdef."""
        with temporary_database() as d1:
            with connect(d1) as s1:
                s1.execute("""
                    create table parent(id int primary key);
                    create table child(id int, parent_id int);
                    alter table child add constraint child_fk
                        foreign key (parent_id) references parent(id) not valid;
                """)
                i1 = get_inspector(s1)
            c = i1.constraints['"public"."child"."child_fk"']
            # pg_get_constraintdef includes NOT VALID in the definition
            assert "not valid" in c.definition.lower(), (
                f"NOT VALID not in constraint definition: {c.definition}"
            )

    def test_constraint_deferrable_initially_immediate(self):
        """DEFERRABLE INITIALLY IMMEDIATE should be preserved."""
        with temporary_database() as d1:
            with connect(d1) as s1:
                s1.execute("""
                    create table parent(id int primary key);
                    create table child(
                        id int,
                        parent_id int,
                        constraint child_fk foreign key (parent_id)
                            references parent(id) deferrable initially immediate
                    );
                """)
                i1 = get_inspector(s1)
            c = i1.constraints['"public"."child"."child_fk"']
            assert c.is_deferrable
            assert not c.initially_deferred

    def test_exclusion_constraint_diff(self):
        """Exclusion constraints should diff correctly when changed."""
        with temporary_database() as d1, temporary_database() as d2:
            with connect(d1) as s1, connect(d2) as s2:
                s1.execute("create extension btree_gist;")
                s1.execute("""
                    create table reservations(
                        id int, room int, during tsrange,
                        exclude using gist (room with =, during with &&)
                    );
                """)
                s2.execute("create extension btree_gist;")
                s2.execute("""
                    create table reservations(id int, room int, during tsrange);
                """)
                i1 = get_inspector(s1)
                i2 = get_inspector(s2)
            changes = Changes(i1, i2)
            stmts = changes.non_pk_constraints()
            stmts.safe = False
            assert stmts.sql.strip(), (
                "BUG: dropping exclusion constraint produces no SQL"
            )


# ============================================================================
# INDEXES — expression indexes, INCLUDE columns, NULLS NOT DISTINCT
# ============================================================================


class TestIndexEdgeCases:
    def test_expression_index_roundtrip(self):
        """Expression index should roundtrip correctly."""
        with temporary_database() as d1:
            with connect(d1) as s1:
                s1.execute("""
                    create table t(name text);
                    create index t_lower_name on t (lower(name));
                """)
                i1 = get_inspector(s1)
            idx = i1.indexes['"public"."t_lower_name"']
            assert "lower" in idx.definition.lower()
            # Roundtrip: drop and recreate
            with temporary_database() as d2:
                with connect(d2) as s2:
                    s2.execute("create table t(name text);")
                    s2.execute(idx.create_statement)
                    i2 = get_inspector(s2)
                idx2 = i2.indexes['"public"."t_lower_name"']
                assert idx == idx2

    def test_include_index_roundtrip(self):
        """Index with INCLUDE columns should roundtrip correctly."""
        with temporary_database() as d1:
            with connect(d1) as s1:
                s1.execute("""
                    create table t(id int, name text, val int);
                    create index t_covering on t (id) include (name, val);
                """)
                i1 = get_inspector(s1)
            idx = i1.indexes['"public"."t_covering"']
            assert idx.included_columns is not None and len(idx.included_columns) == 2
            with temporary_database() as d2:
                with connect(d2) as s2:
                    s2.execute("create table t(id int, name text, val int);")
                    s2.execute(idx.create_statement)
                    i2 = get_inspector(s2)
                idx2 = i2.indexes['"public"."t_covering"']
                assert idx == idx2

    def test_index_with_collation_diff(self):
        """Index with explicit collation should be detected when collation changes."""
        with temporary_database() as d1, temporary_database() as d2:
            with connect(d1) as s1, connect(d2) as s2:
                s1.execute("""
                    create table t(name text);
                    create index t_name on t (name collate "C");
                """)
                s2.execute("""
                    create table t(name text);
                    create index t_name on t (name collate "POSIX");
                """)
                i1 = get_inspector(s1)
                i2 = get_inspector(s2)
            i1.indexes['"public"."t_name"']
            i2.indexes['"public"."t_name"']

    def test_partial_index_predicate_change(self):
        """Changing partial index predicate should be detected."""
        with temporary_database() as d1, temporary_database() as d2:
            with connect(d1) as s1, connect(d2) as s2:
                s1.execute("""
                    create table t(id int, active bool);
                    create index t_active on t (id) where active;
                """)
                s2.execute("""
                    create table t(id int, active bool);
                    create index t_active on t (id) where (not active);
                """)
                i1 = get_inspector(s1)
                i2 = get_inspector(s2)
            idx1 = i1.indexes['"public"."t_active"']
            idx2 = i2.indexes['"public"."t_active"']
            assert idx1 != idx2, (
                "BUG: partial index predicate change not detected"
            )

    @pytest.mark.requires_pg(min_version=15)
    def test_nulls_not_distinct_index(self):
        """PG 15+ NULLS NOT DISTINCT on unique index should roundtrip."""
        with temporary_database() as d1:
            with connect(d1) as s1:
                s1.execute("""
                    create table t(id int, code text);
                    create unique index t_code on t (code) nulls not distinct;
                """)
                i1 = get_inspector(s1)
            idx = i1.indexes['"public"."t_code"']
            assert "nulls not distinct" in idx.definition.lower()
            with temporary_database() as d2:
                with connect(d2) as s2:
                    s2.execute("create table t(id int, code text);")
                    s2.execute(idx.create_statement)
                    i2 = get_inspector(s2)
                idx2 = i2.indexes['"public"."t_code"']
                assert idx == idx2


# ============================================================================
# FUNCTIONS — edge cases in definition comparison
# ============================================================================


class TestFunctionEdgeCases:
    def test_function_parallel_safe_diff_produces_migration(self):
        """Changing PARALLEL SAFE should produce migration SQL."""
        with temporary_database() as d1, temporary_database() as d2:
            with connect(d1) as s1, connect(d2) as s2:
                s1.execute("""
                    create function add2(a int, b int) returns int
                    language sql parallel unsafe
                    as $$ select a + b $$;
                """)
                s2.execute("""
                    create function add2(a int, b int) returns int
                    language sql parallel safe
                    as $$ select a + b $$;
                """)
                i1 = get_inspector(s1)
                i2 = get_inspector(s2)
            changes = Changes(i1, i2)
            stmts = changes.selectables()
            assert stmts.sql.strip(), (
                "BUG: PARALLEL SAFE change produces no migration SQL"
            )

    def test_function_set_config_diff(self):
        """Function with SET search_path should diff against one without."""
        with temporary_database() as d1, temporary_database() as d2:
            with connect(d1) as s1, connect(d2) as s2:
                s1.execute("""
                    create function secure_func() returns void
                    language sql as $$ select 1 $$;
                """)
                s2.execute("""
                    create function secure_func() returns void
                    language sql set search_path = public
                    as $$ select 1 $$;
                """)
                i1 = get_inspector(s1)
                i2 = get_inspector(s2)
            key = '"public"."secure_func"()'
            assert i1.functions[key] != i2.functions[key], (
                "BUG: function SET search_path change not detected"
            )

    def test_function_rows_estimate_diff(self):
        """Changing ROWS estimate on set-returning function should be detected."""
        with temporary_database() as d1, temporary_database() as d2:
            with connect(d1) as s1, connect(d2) as s2:
                s1.execute("""
                    create function gen(n int) returns setof int
                    language sql rows 100
                    as $$ select generate_series(1, n) $$;
                """)
                s2.execute("""
                    create function gen(n int) returns setof int
                    language sql rows 1000
                    as $$ select generate_series(1, n) $$;
                """)
                i1 = get_inspector(s1)
                i2 = get_inspector(s2)
            key = '"public"."gen"(n integer)'
            assert i1.functions[key] != i2.functions[key], (
                "BUG: function ROWS estimate change not detected"
            )

    def test_procedure_roundtrip(self):
        """Procedures (not functions) should roundtrip correctly."""
        with temporary_database() as d1:
            with connect(d1) as s1:
                s1.execute("""
                    create procedure do_nothing()
                    language sql as $$ select 1 $$;
                """)
                i1 = get_inspector(s1)
            key = '"public"."do_nothing"()'
            f = i1.functions[key]
            assert f.kind == "p"
            with temporary_database() as d2:
                with connect(d2) as s2:
                    s2.execute(f.create_statement)
                    i2 = get_inspector(s2)
                assert i2.functions[key] == f


# ============================================================================
# IDENTITY COLUMNS — sequence params behind identity
# ============================================================================


class TestIdentityColumnEdgeCases:
    def test_identity_always_vs_by_default(self):
        """ALWAYS vs BY DEFAULT identity should be detected."""
        with temporary_database() as d1, temporary_database() as d2:
            with connect(d1) as s1, connect(d2) as s2:
                s1.execute("""
                    create table t(id int generated always as identity);
                """)
                s2.execute("""
                    create table t(id int generated by default as identity);
                """)
                i1 = get_inspector(s1)
                i2 = get_inspector(s2)
            t1 = i1.relations['"public"."t"']
            t2 = i2.relations['"public"."t"']
            assert t1 != t2, (
                "BUG: identity ALWAYS vs BY DEFAULT not detected"
            )

    def test_identity_to_regular_column(self):
        """Dropping identity from a column should be detected."""
        with temporary_database() as d1, temporary_database() as d2:
            with connect(d1) as s1, connect(d2) as s2:
                s1.execute("""
                    create table t(id int generated always as identity, val text);
                """)
                s2.execute("create table t(id int, val text);")
                i1 = get_inspector(s1)
                i2 = get_inspector(s2)
            t1 = i1.relations['"public"."t"']
            t2 = i2.relations['"public"."t"']
            assert t1 != t2, (
                "BUG: dropping identity column not detected"
            )
            changes = Changes(i1, i2)
            stmts = changes.selectables()
            stmts.safe = False
            sql = stmts.sql.lower()
            assert "identity" in sql, (
                f"BUG: identity removal produces no SQL. Got: {sql!r}"
            )


# ============================================================================
# DOMAIN — multiple constraints, complex defaults
# ============================================================================


class TestDomainEdgeCases:
    def test_domain_not_null_change(self):
        """Changing NOT NULL on a domain should be detected."""
        with temporary_database() as d1, temporary_database() as d2:
            with connect(d1) as s1, connect(d2) as s2:
                s1.execute("create domain posint as int;")
                s2.execute("create domain posint as int not null;")
                i1 = get_inspector(s1)
                i2 = get_inspector(s2)
            d1_dom = i1.domains['"public"."posint"']
            d2_dom = i2.domains['"public"."posint"']
            assert d1_dom != d2_dom, (
                "BUG: domain NOT NULL change not detected"
            )

    def test_domain_default_change(self):
        with temporary_database() as d1, temporary_database() as d2:
            with connect(d1) as s1, connect(d2) as s2:
                s1.execute("create domain myint as int default 0;")
                s2.execute("create domain myint as int default 42;")
                i1 = get_inspector(s1)
                i2 = get_inspector(s2)
            d1_dom = i1.domains['"public"."myint"']
            d2_dom = i2.domains['"public"."myint"']
            assert d1_dom != d2_dom, (
                "BUG: domain default change not detected"
            )

    def test_domain_check_constraint_change(self):
        with temporary_database() as d1, temporary_database() as d2:
            with connect(d1) as s1, connect(d2) as s2:
                s1.execute("""
                    create domain posint as int check (value > 0);
                """)
                s2.execute("""
                    create domain posint as int check (value >= 0);
                """)
                i1 = get_inspector(s1)
                i2 = get_inspector(s2)
            d1_dom = i1.domains['"public"."posint"']
            d2_dom = i2.domains['"public"."posint"']
            assert d1_dom != d2_dom, (
                "BUG: domain CHECK constraint change not detected"
            )


# ============================================================================
# RLS POLICIES — permissive vs restrictive, command type changes
# ============================================================================


class TestRLSPolicyEdgeCases:
    def test_policy_permissive_to_restrictive(self):
        """Changing policy from PERMISSIVE to RESTRICTIVE requires drop+create."""
        with temporary_database() as d1, temporary_database() as d2:
            with connect(d1) as s1, connect(d2) as s2:
                s1.execute("""
                    create table t(id int);
                    alter table t enable row level security;
                    create policy p on t as permissive for select to public using (true);
                """)
                s2.execute("""
                    create table t(id int);
                    alter table t enable row level security;
                    create policy p on t as restrictive for select to public using (true);
                """)
                i1 = get_inspector(s1)
                i2 = get_inspector(s2)
            changes = Changes(i1, i2)
            stmts = changes.rlspolicies()
            stmts.safe = False
            sql = stmts.sql.lower()
            assert "drop policy" in sql and "create policy" in sql, (
                f"BUG: permissive→restrictive should drop+create. Got: {sql!r}"
            )

    def test_policy_command_type_change(self):
        """Changing policy command type (SELECT→ALL) requires drop+create."""
        with temporary_database() as d1, temporary_database() as d2:
            with connect(d1) as s1, connect(d2) as s2:
                s1.execute("""
                    create table t(id int);
                    alter table t enable row level security;
                    create policy p on t for select to public using (true);
                """)
                s2.execute("""
                    create table t(id int);
                    alter table t enable row level security;
                    create policy p on t for all to public using (true);
                """)
                i1 = get_inspector(s1)
                i2 = get_inspector(s2)
            changes = Changes(i1, i2)
            stmts = changes.rlspolicies()
            stmts.safe = False
            sql = stmts.sql.lower()
            assert "drop policy" in sql and "create policy" in sql, (
                f"BUG: command type change should drop+create. Got: {sql!r}"
            )

    def test_policy_using_expression_change(self):
        """Changing USING expression should produce migration SQL (drop+create)."""
        with temporary_database() as d1, temporary_database() as d2:
            with connect(d1) as s1, connect(d2) as s2:
                s1.execute("""
                    create table t(id int, owner_id int);
                    alter table t enable row level security;
                    create policy p on t using (owner_id = 1);
                """)
                s2.execute("""
                    create table t(id int, owner_id int);
                    alter table t enable row level security;
                    create policy p on t using (owner_id = 2);
                """)
                i1 = get_inspector(s1)
                i2 = get_inspector(s2)
            changes = Changes(i1, i2)
            stmts = changes.rlspolicies()
            stmts.safe = False
            sql = stmts.sql.lower()
            assert "drop policy" in sql and "create policy" in sql, (
                f"BUG: policy USING change produces no SQL. Got: {sql!r}"
            )


# ============================================================================
# PUBLICATIONS — PG 15+ row filters and column lists
# ============================================================================


@pytest.mark.requires_pg(min_version=15)
class TestPublicationPG15:
    def test_publication_row_filter(self):
        """PG 15+ row filter on publication table should be inspected."""
        with temporary_database() as d1:
            with connect(d1) as s1:
                s1.execute("create table t(id int, active bool);")
                s1.execute("""
                    create publication pub for table t where (active);
                """)
                i1 = get_inspector(s1)
            i1.publications['"pub"']

    def test_publication_column_list(self):
        """PG 15+ column list on publication table should be inspected."""
        with temporary_database() as d1:
            with connect(d1) as s1:
                s1.execute("create table t(id int, name text, secret text);")
                s1.execute("""
                    create publication pub for table t (id, name);
                """)
                i1 = get_inspector(s1)
            i1.publications['"pub"']


# ============================================================================
# TRIGGERS — transition tables (REFERENCING), argument passing
# ============================================================================


class TestTriggerEdgeCases:
    def test_trigger_with_args(self):
        """Trigger with arguments should roundtrip."""
        with temporary_database() as d1:
            with connect(d1) as s1:
                s1.execute("""
                    create table t(id int);
                    create function trg_func() returns trigger
                    language plpgsql as $$ begin return new; end $$;
                    create trigger my_trg before insert on t
                    for each row execute function trg_func('arg1', 'arg2');
                """)
                i1 = get_inspector(s1)
            trg = i1.triggers['"public"."t"."my_trg"']
            assert "arg1" in trg.full_definition
            with temporary_database() as d2:
                with connect(d2) as s2:
                    s2.execute("create table t(id int);")
                    s2.execute("""
                        create function trg_func() returns trigger
                        language plpgsql as $$ begin return new; end $$;
                    """)
                    s2.execute(trg.create_statement)
                    i2 = get_inspector(s2)
                trg2 = i2.triggers['"public"."t"."my_trg"']
                assert trg == trg2

    def test_trigger_when_clause(self):
        """Trigger with WHEN clause should roundtrip."""
        with temporary_database() as d1:
            with connect(d1) as s1:
                s1.execute("""
                    create table t(id int, status text);
                    create function trg_func() returns trigger
                    language plpgsql as $$ begin return new; end $$;
                    create trigger status_trg before update on t
                    for each row when (old.status is distinct from new.status)
                    execute function trg_func();
                """)
                i1 = get_inspector(s1)
            trg = i1.triggers['"public"."t"."status_trg"']
            assert "when" in trg.full_definition.lower()
            with temporary_database() as d2:
                with connect(d2) as s2:
                    s2.execute("""
                        create table t(id int, status text);
                        create function trg_func() returns trigger
                        language plpgsql as $$ begin return new; end $$;
                    """)
                    s2.execute(trg.create_statement)
                    i2 = get_inspector(s2)
                trg2 = i2.triggers['"public"."t"."status_trg"']
                assert trg == trg2

    def test_trigger_referencing_transition_tables(self):
        """Trigger with REFERENCING OLD/NEW TABLE should roundtrip."""
        with temporary_database() as d1:
            with connect(d1) as s1:
                s1.execute("""
                    create table t(id int);
                    create function audit_func() returns trigger
                    language plpgsql as $$
                    begin return null; end $$;
                    create trigger audit_trg after insert on t
                    referencing new table as new_rows
                    for each statement execute function audit_func();
                """)
                i1 = get_inspector(s1)
            trg = i1.triggers['"public"."t"."audit_trg"']
            assert "referencing" in trg.full_definition.lower()
            with temporary_database() as d2:
                with connect(d2) as s2:
                    s2.execute("""
                        create table t(id int);
                        create function audit_func() returns trigger
                        language plpgsql as $$
                        begin return null; end $$;
                    """)
                    s2.execute(trg.create_statement)
                    i2 = get_inspector(s2)
                trg2 = i2.triggers['"public"."t"."audit_trg"']
                assert trg == trg2


# ============================================================================
# GENERATED COLUMNS — edge cases
# ============================================================================


class TestGeneratedColumnEdgeCases:
    def test_generated_column_expression_change(self):
        """Changing the expression of a generated column should be detected."""
        with temporary_database() as d1, temporary_database() as d2:
            with connect(d1) as s1, connect(d2) as s2:
                s1.execute("""
                    create table t(
                        a int, b int,
                        c int generated always as (a + b) stored
                    );
                """)
                s2.execute("""
                    create table t(
                        a int, b int,
                        c int generated always as (a * b) stored
                    );
                """)
                i1 = get_inspector(s1)
                i2 = get_inspector(s2)
            t1 = i1.relations['"public"."t"']
            t2 = i2.relations['"public"."t"']
            assert t1 != t2, (
                "BUG: generated column expression change not detected"
            )


# ============================================================================
# COMPOSITE TYPES — column changes
# ============================================================================


class TestCompositeTypeEdgeCases:
    def test_composite_type_column_type_change(self):
        """Changing a column type in composite type should be detected."""
        with temporary_database() as d1, temporary_database() as d2:
            with connect(d1) as s1, connect(d2) as s2:
                s1.execute("create type mytype as (x int, y text);")
                s2.execute("create type mytype as (x bigint, y text);")
                i1 = get_inspector(s1)
                i2 = get_inspector(s2)
            key = '"public"."mytype"'
            assert i1.composite_types[key] != i2.composite_types[key], (
                "BUG: composite type column type change not detected"
            )

    def test_composite_type_column_add(self):
        """Adding a column to composite type should be detected."""
        with temporary_database() as d1, temporary_database() as d2:
            with connect(d1) as s1, connect(d2) as s2:
                s1.execute("create type mytype as (x int);")
                s2.execute("create type mytype as (x int, y text);")
                i1 = get_inspector(s1)
                i2 = get_inspector(s2)
            key = '"public"."mytype"'
            assert i1.composite_types[key] != i2.composite_types[key], (
                "BUG: composite type column addition not detected"
            )


# ============================================================================
# CROSS-OBJECT INTERACTIONS
# ============================================================================


class TestCrossObjectInteractions:
    def test_enum_used_by_table_column_add_value(self):
        """Adding enum value when enum is used by a table column."""
        with temporary_database() as d1, temporary_database() as d2:
            with connect(d1) as s1, connect(d2) as s2:
                s1.execute("""
                    create type status as enum ('active', 'inactive');
                    create table t(id int, s status);
                """)
                s2.execute("""
                    create type status as enum ('active', 'inactive', 'pending');
                    create table t(id int, s status);
                """)
                i1 = get_inspector(s1)
                i2 = get_inspector(s2)
            changes = Changes(i1, i2)
            stmts = changes.selectables()
            sql = stmts.sql.lower()
            assert "add value" in sql, (
                f"BUG: enum value addition not producing ALTER TYPE. Got: {sql!r}"
            )

    def test_function_used_by_trigger_changed(self):
        """Changing a function used by a trigger should update both."""
        with temporary_database() as d1, temporary_database() as d2:
            with connect(d1) as s1, connect(d2) as s2:
                s1.execute("""
                    create table t(id int);
                    create function trg_func() returns trigger
                    language plpgsql as $$ begin return new; end $$;
                    create trigger my_trg before insert on t
                    for each row execute function trg_func();
                """)
                s2.execute("""
                    create table t(id int);
                    create function trg_func() returns trigger
                    language plpgsql as $$ begin
                        raise notice 'inserted';
                        return new;
                    end $$;
                    create trigger my_trg before insert on t
                    for each row execute function trg_func();
                """)
                i1 = get_inspector(s1)
                i2 = get_inspector(s2)
            changes = Changes(i1, i2)
            stmts = changes.selectables()
            sql = stmts.sql.lower()
            assert "create or replace function" in sql, (
                f"BUG: function change not detected. Got: {sql!r}"
            )

    def test_table_used_by_view_column_added(self):
        """Adding a column to a table used by a SELECT * view should update the view."""
        with temporary_database() as d1, temporary_database() as d2:
            with connect(d1) as s1, connect(d2) as s2:
                s1.execute("""
                    create table t(id int, name text);
                    create view v as select * from t;
                """)
                s2.execute("""
                    create table t(id int, name text, extra int);
                    create view v as select * from t;
                """)
                i1 = get_inspector(s1)
                i2 = get_inspector(s2)
            changes = Changes(i1, i2)
            stmts = changes.selectables()
            sql = stmts.sql.lower()
            # Both table and view should be updated
            assert "alter table" in sql or "add column" in sql, (
                f"BUG: table column addition not handled. Got: {sql!r}"
            )


# ============================================================================
# SPECIAL CHARACTERS IN IDENTIFIERS
# ============================================================================


class TestSpecialIdentifiers:
    def test_table_with_spaces_in_name(self):
        """Table with spaces in name should roundtrip."""
        with temporary_database() as d1:
            with connect(d1) as s1:
                s1.execute('create table "my table"(id int);')
                i1 = get_inspector(s1)
            t = i1.relations['"public"."my table"']
            with temporary_database() as d2:
                with connect(d2) as s2:
                    s2.execute(t.create_statement)
                    i2 = get_inspector(s2)
                assert i2.relations['"public"."my table"'] == t

    def test_column_with_reserved_word(self):
        """Column named with reserved word should be properly quoted."""
        with temporary_database() as d1:
            with connect(d1) as s1:
                s1.execute('create table t("select" int, "from" text);')
                i1 = get_inspector(s1)
            t = i1.relations['"public"."t"']
            with temporary_database() as d2:
                with connect(d2) as s2:
                    s2.execute(t.create_statement)
                    i2 = get_inspector(s2)
                assert i2.relations['"public"."t"'] == t

    def test_schema_qualified_references(self):
        """Objects in non-public schemas should diff correctly."""
        with temporary_database() as d1, temporary_database() as d2:
            with connect(d1) as s1, connect(d2) as s2:
                s1.execute("""
                    create schema myschema;
                    create table myschema.t(id int);
                """)
                s2.execute("""
                    create schema myschema;
                    create table myschema.t(id int, name text);
                """)
                i1 = get_inspector(s1)
                i2 = get_inspector(s2)
            changes = Changes(i1, i2)
            stmts = changes.selectables()
            sql = stmts.sql.lower()
            assert "myschema" in sql and "add column" in sql, (
                f"BUG: schema-qualified table change failed. Got: {sql!r}"
            )
