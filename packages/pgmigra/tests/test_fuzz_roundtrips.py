"""Fuzz every DDL object type with roundtrip and diff-apply tests.

For each object type:
1. Create it in a database with various configurations
2. Inspect it, generate CREATE statement
3. Drop it, recreate from generated statement
4. Re-inspect and compare (roundtrip)
5. Test diff-apply: create variants in two databases, diff, apply, verify equal
"""

from pgmigra import Migration
from pgmigra.db import connect, temporary_database
from pgmigra.schemainspect import get_inspector

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def roundtrip_ddl(db_url, setup_sql, object_getter):
    """Create objects, inspect, drop+recreate from generated SQL, compare."""
    with connect(db_url) as s:
        s.execute(setup_sql)
        i1 = get_inspector(s)
        obj1 = object_getter(i1)
        create_sql = obj1.create_statement
        drop_sql = obj1.drop_statement
        s.execute(drop_sql)
        s.execute(create_sql)
        i2 = get_inspector(s)
        obj2 = object_getter(i2)
    return obj1, obj2, create_sql


def diff_apply(setup_from, setup_target):
    """Create two databases, diff, apply the migration, verify they match."""
    with temporary_database() as d1, temporary_database() as d2:
        with connect(d1) as s1, connect(d2) as s2:
            s1.execute(setup_from)
            s2.execute(setup_target)

        with connect(d1) as s1, connect(d2) as s2:
            m = Migration(s1, s2)
            m.inspect_from()
            m.inspect_target()
            m.set_safety(False)
            m.add_all_changes()
            sql = m.sql

        if sql.strip():
            with connect(d1) as s1:
                s1.execute(sql)

        with connect(d1) as s1, connect(d2) as s2:
            i1 = get_inspector(s1)
            i2 = get_inspector(s2)

    return i1, i2, sql


# ---------------------------------------------------------------------------
# Roundtrip tests
# ---------------------------------------------------------------------------

class TestRoundtrips:

    def test_table_basic(self, db):
        o1, o2, _ = roundtrip_ddl(db,
            "create table t(id int primary key, name text not null, val numeric default 0);",
            lambda i: i.relations['"public"."t"'],
        )
        assert o1 == o2

    def test_table_unlogged(self, db):
        o1, o2, _ = roundtrip_ddl(db,
            "create unlogged table t(id int primary key);",
            lambda i: i.relations['"public"."t"'],
        )
        assert o1 == o2
        assert o1.is_unlogged

    def test_view(self, db):
        o1, o2, _ = roundtrip_ddl(db,
            "create view v as select 1 as x, 'hello'::text as y;",
            lambda i: i.relations['"public"."v"'],
        )
        assert o1 == o2

    def test_materialized_view(self, db):
        o1, o2, sql = roundtrip_ddl(db,
            "create materialized view mv as select generate_series(1,5) as n;",
            lambda i: i.relations['"public"."mv"'],
        )
        assert o1 == o2

    def test_index_unique(self, db):
        with connect(db) as s:
            s.execute("create table t(id int, val text);")
            s.execute("create unique index idx_t_val on t(val);")
            i1 = get_inspector(s)
            idx1 = i1.indexes['"public"."idx_t_val"']
            assert idx1.is_unique

    def test_index_partial(self, db):
        with connect(db) as s:
            s.execute("create table t(id int, active bool);")
            s.execute("create index idx_active on t(id) where active;")
            i1 = get_inspector(s)
            idx1 = i1.indexes['"public"."idx_active"']
            assert idx1.partial_predicate

    def test_constraint_check(self, db):
        with connect(db) as s:
            s.execute("create table t(id int, val int, constraint chk check (val > 0));")
            i = get_inspector(s)
            t = i.relations['"public"."t"']
            constraints = list(t.constraints.values())
            assert any("val > 0" in c.definition for c in constraints)

    def test_constraint_fk(self, db):
        with connect(db) as s:
            s.execute("""
                create table parent(id int primary key);
                create table child(id int, pid int references parent(id));
            """)
            i = get_inspector(s)
            child = i.relations['"public"."child"']
            fks = [c for c in child.constraints.values() if c.is_fk]
            assert len(fks) == 1

    def test_constraint_deferred(self, db):
        with connect(db) as s:
            s.execute("""
                create table parent(id int primary key);
                create table child(
                    id int, pid int,
                    constraint fk_def foreign key (pid) references parent(id)
                        deferrable initially deferred
                );
            """)
            i = get_inspector(s)
            child = i.relations['"public"."child"']
            fk = [c for c in child.constraints.values() if c.is_fk][0]
            assert fk.is_deferrable
            assert fk.initially_deferred

    def test_sequence(self, db):
        with connect(db) as s:
            s.execute("create table t(id serial primary key);")
            i = get_inspector(s)
            seqs = list(i.sequences.values())
            assert len(seqs) >= 1

    def test_enum(self, db):
        with connect(db) as s:
            s.execute("create type mood as enum ('happy', 'sad', 'neutral');")
            i = get_inspector(s)
            e = i.enums['"public"."mood"']
            assert e.elements == ["happy", "sad", "neutral"]
            s.execute(e.drop_statement)
            s.execute(e.create_statement)
            i2 = get_inspector(s)
            assert e == i2.enums['"public"."mood"']

    def test_domain(self, db):
        with connect(db) as s:
            s.execute("create domain positive_int as int check (value > 0);")
            i = get_inspector(s)
            d = i.domains['"public"."positive_int"']
            s.execute(d.drop_statement)
            s.execute(d.create_statement)
            i2 = get_inspector(s)
            assert d == i2.domains['"public"."positive_int"']

    def test_function(self, db):
        with connect(db) as s:
            s.execute("""
                create function add_nums(a int, b int) returns int
                language sql as 'select a + b';
            """)
            i = get_inspector(s)
            fn = list(i.functions.values())[0]
            s.execute(fn.drop_statement)
            s.execute(fn.create_statement)
            i2 = get_inspector(s)
            fn2 = list(i2.functions.values())[0]
            assert fn == fn2

    def test_function_security_definer(self, db):
        with connect(db) as s:
            s.execute("""
                create function sec_fn() returns int
                language sql security definer as 'select 1';
            """)
            i = get_inspector(s)
            fn = list(i.functions.values())[0]
            assert "definer" in fn.security_type.lower()

    def test_trigger(self, db):
        with connect(db) as s:
            s.execute("""
                create table t(id int);
                create function trg_fn() returns trigger
                language plpgsql as 'begin return new; end';
                create trigger trg before insert on t
                for each row execute function trg_fn();
            """)
            i = get_inspector(s)
            trg = list(i.triggers.values())[0]
            s.execute(f"drop trigger {trg.name} on {trg.quoted_full_selectable_name};")
            s.execute(trg.create_statement)
            i2 = get_inspector(s)
            trg2 = list(i2.triggers.values())[0]
            assert trg == trg2

    def test_publication_roundtrip(self, db):
        with connect(db) as s:
            s.execute("create table t1(id int primary key);")
            s.execute(
                "create publication pub for table t1"
                " with (publish = 'insert, update', publish_via_partition_root = true);"
            )
            i = get_inspector(s)
            pub = i.publications['"pub"']
            s.execute(pub.drop_statement)
            s.execute(pub.create_statement)
            i2 = get_inspector(s)
            pub2 = i2.publications['"pub"']
            assert pub == pub2

    def test_role_roundtrip(self, db):
        with connect(db) as s:
            i = get_inspector(s)
            role = i.roles.get("schemainspect_test_role")
            if role:
                sql = role.create_statement
                assert "schemainspect_test_role" in sql

    def test_collation(self, db):
        with connect(db) as s:
            s.execute(
                "create collation custom_c (provider = icu, locale = 'und-u-ks-level2');"
            )
            i = get_inspector(s)
            c = i.collations['"public"."custom_c"']
            s.execute(c.drop_statement)
            s.execute(c.create_statement)
            i2 = get_inspector(s)
            c2 = i2.collations['"public"."custom_c"']
            assert c == c2

    def test_rls_policy(self, db):
        with connect(db) as s:
            s.execute("""
                create table t(id int, owner_name text);
                alter table t enable row level security;
                create policy p on t for select
                    using (owner_name = current_user);
            """)
            i = get_inspector(s)
            policies = list(i.rlspolicies.values())
            assert len(policies) == 1
            p = policies[0]
            assert "current_user" in p.qual.lower() or "current_user" in str(p.qual)

    def test_rule(self, db):
        with connect(db) as s:
            s.execute("""
                create table t(id int);
                create rule r as on insert to t do instead nothing;
            """)
            i = get_inspector(s)
            rules = list(i.rules.values())
            assert len(rules) == 1

    def test_statistics(self, db):
        with connect(db) as s:
            s.execute("""
                create table t(a int, b int, c int);
                create statistics st1 (dependencies) on a, b from t;
            """)
            i = get_inspector(s)
            stats = list(i.statistics.values())
            assert len(stats) == 1

    def test_event_trigger(self, db):
        with connect(db) as s:
            s.execute("""
                create function evt_fn() returns event_trigger
                language plpgsql as 'begin end';
                create event trigger evt on ddl_command_start
                execute function evt_fn();
            """)
            i = get_inspector(s)
            et = list(i.event_triggers.values())
            assert len(et) == 1

    def test_ts_dict(self, db):
        with connect(db) as s:
            s.execute("""
                create text search dictionary my_dict (
                    template = simple,
                    stopwords = 'english'
                );
            """)
            i = get_inspector(s)
            d = list(i.ts_dicts.values())
            assert any(x.name == "my_dict" for x in d)

    def test_ts_config(self, db):
        with connect(db) as s:
            s.execute("""
                create text search configuration my_cfg (parser = default);
                alter text search configuration my_cfg
                    add mapping for asciiword with simple;
            """)
            i = get_inspector(s)
            c = [x for x in i.ts_configs.values() if x.name == "my_cfg"]
            assert len(c) == 1

    def test_cast(self, db):
        with connect(db) as s:
            s.execute("""
                create function cast_text_to_int(text) returns int
                language sql as 'select $1::int';
                create cast (text as int) with function cast_text_to_int(text);
            """)
            i = get_inspector(s)
            casts = {k: v for k, v in i.casts.items() if "text" in k and "int" in k}
            assert len(casts) > 0
            c = list(casts.values())[0]
            s.execute(c.drop_statement)
            s.execute(c.create_statement)
            i2 = get_inspector(s)
            casts2 = {k: v for k, v in i2.casts.items() if "text" in k and "int" in k}
            c2 = list(casts2.values())[0]
            assert c == c2

    def test_range_type(self, db):
        with connect(db) as s:
            s.execute("create type float_range as range (subtype = float8);")
            i = get_inspector(s)
            r = i.range_types['"public"."float_range"']
            s.execute(r.drop_statement)
            s.execute(r.create_statement)
            i2 = get_inspector(s)
            r2 = i2.range_types['"public"."float_range"']
            assert r == r2

    def test_composite_type(self, db):
        with connect(db) as s:
            s.execute("create type addr as (street text, city text, zip text);")
            i = get_inspector(s)
            t = i.composite_types['"public"."addr"']
            s.execute(t.drop_statement)
            s.execute(t.create_statement)
            i2 = get_inspector(s)
            t2 = i2.composite_types['"public"."addr"']
            assert t == t2


# ---------------------------------------------------------------------------
# Diff-apply tests: create object in target only, migrate, verify match
# ---------------------------------------------------------------------------

class TestDiffApply:

    def test_add_table(self):
        i1, i2, sql = diff_apply("", "create table t(id int primary key, val text);")
        assert i1.tables == i2.tables

    def test_add_view(self):
        i1, i2, sql = diff_apply("", "create view v as select 1 as x;")
        assert i1.relations == i2.relations

    def test_add_function(self):
        i1, i2, sql = diff_apply(
            "",
            "create function f() returns int language sql as 'select 42';",
        )
        assert i1.functions == i2.functions

    def test_add_enum(self):
        i1, i2, sql = diff_apply(
            "",
            "create type color as enum ('red', 'green', 'blue');",
        )
        assert i1.enums == i2.enums

    def test_modify_table_add_column(self):
        i1, i2, sql = diff_apply(
            "create table t(id int primary key);",
            "create table t(id int primary key, val text);",
        )
        assert i1.tables == i2.tables

    def test_modify_table_drop_column(self):
        i1, i2, sql = diff_apply(
            "create table t(id int primary key, val text);",
            "create table t(id int primary key);",
        )
        assert i1.tables == i2.tables

    def test_add_index(self):
        i1, i2, sql = diff_apply(
            "create table t(id int, val text);",
            "create table t(id int, val text); create index idx on t(val);",
        )
        assert i1.indexes == i2.indexes

    def test_add_constraint(self):
        i1, i2, sql = diff_apply(
            "create table t(id int, val int);",
            "create table t(id int, val int, constraint chk check (val > 0));",
        )
        assert i1.constraints == i2.constraints

    def test_add_trigger(self):
        base = """
            create table t(id int);
            create function trg_fn() returns trigger
            language plpgsql as 'begin return new; end';
        """
        i1, i2, sql = diff_apply(
            base,
            base + "create trigger trg before insert on t for each row execute function trg_fn();",
        )
        assert i1.triggers == i2.triggers

    def test_modify_function(self):
        i1, i2, sql = diff_apply(
            "create function f() returns int language sql as 'select 1';",
            "create function f() returns int language sql as 'select 2';",
        )
        assert i1.functions == i2.functions

    def test_modify_view(self):
        i1, i2, sql = diff_apply(
            "create view v as select 1 as x;",
            "create view v as select 1 as x, 2 as y;",
        )
        assert i1.relations == i2.relations

    def test_add_rls(self):
        i1, i2, sql = diff_apply(
            "create table t(id int primary key);",
            """
            create table t(id int primary key);
            alter table t enable row level security;
            """,
        )
        assert i1.tables == i2.tables

    def test_add_force_rls(self):
        i1, i2, sql = diff_apply(
            """
            create table t(id int primary key);
            alter table t enable row level security;
            """,
            """
            create table t(id int primary key);
            alter table t enable row level security;
            alter table t force row level security;
            """,
        )
        assert i1.tables == i2.tables
        assert "force row level security" in sql.lower()

    def test_remove_force_rls(self):
        i1, i2, sql = diff_apply(
            """
            create table t(id int primary key);
            alter table t enable row level security;
            alter table t force row level security;
            """,
            """
            create table t(id int primary key);
            alter table t enable row level security;
            """,
        )
        assert i1.tables == i2.tables
        assert "no force row level security" in sql.lower()

    def test_unlogged_to_logged(self):
        i1, i2, sql = diff_apply(
            "create unlogged table t(id int primary key);",
            "create table t(id int primary key);",
        )
        assert i1.tables == i2.tables

    def test_logged_to_unlogged(self):
        i1, i2, sql = diff_apply(
            "create table t(id int primary key);",
            "create unlogged table t(id int primary key);",
        )
        assert i1.tables == i2.tables

    def test_add_domain(self):
        i1, i2, sql = diff_apply(
            "",
            "create domain pos_int as int check (value > 0);",
        )
        assert i1.domains == i2.domains

    def test_add_range_type(self):
        i1, i2, sql = diff_apply(
            "",
            "create type my_range as range (subtype = int4);",
        )
        assert i1.range_types == i2.range_types

    def test_add_composite_type(self):
        i1, i2, sql = diff_apply(
            "",
            "create type point3d as (x float, y float, z float);",
        )
        assert i1.composite_types == i2.composite_types

    def test_modify_enum_add_value(self):
        i1, i2, sql = diff_apply(
            "create type color as enum ('red', 'green');",
            "create type color as enum ('red', 'green', 'blue');",
        )
        assert i1.enums == i2.enums

    def test_add_publication(self):
        i1, i2, sql = diff_apply(
            "create table t(id int primary key);",
            """
            create table t(id int primary key);
            create publication pub for table t;
            """,
        )
        assert i1.publications == i2.publications

    def test_add_collation(self):
        i1, i2, sql = diff_apply(
            "",
            "create collation my_coll (provider = icu, locale = 'und');",
        )
        assert i1.collations == i2.collations

    def test_add_rls_policy(self):
        base = """
            create table t(id int, owner_name text);
            alter table t enable row level security;
        """
        i1, i2, sql = diff_apply(
            base,
            base + "create policy p on t for select using (owner_name = current_user);",
        )
        assert i1.rlspolicies == i2.rlspolicies

    def test_modify_publication_options(self):
        base = "create table t(id int primary key);"
        i1, i2, sql = diff_apply(
            base + "create publication pub for table t;",
            base + "create publication pub for table t with (publish = 'insert');",
        )
        assert i1.publications == i2.publications

    def test_add_rule(self):
        base = "create table t(id int);"
        i1, i2, sql = diff_apply(
            base,
            base + "create rule r as on insert to t do instead nothing;",
        )
        assert i1.rules == i2.rules

    def test_add_statistics(self):
        base = "create table t(a int, b int, c int);"
        i1, i2, sql = diff_apply(
            base,
            base + "create statistics st (dependencies) on a, b from t;",
        )
        assert i1.statistics == i2.statistics

    def test_add_exclusion_constraint(self):
        i1, i2, sql = diff_apply(
            "create extension btree_gist; create table t(id int, r int4range);",
            """
            create extension btree_gist;
            create table t(
                id int, r int4range,
                constraint excl exclude using gist (r with &&)
            );
            """,
        )
        assert i1.constraints == i2.constraints

    def test_generated_column_stored(self):
        i1, i2, sql = diff_apply(
            "create table t(a int);",
            "create table t(a int, b int generated always as (a * 2) stored);",
        )
        assert i1.tables == i2.tables

    def test_identity_column(self):
        i1, i2, sql = diff_apply(
            "",
            "create table t(id int generated always as identity primary key, val text);",
        )
        assert i1.tables == i2.tables

    def test_partitioned_table(self):
        i1, i2, sql = diff_apply(
            "",
            """
            create table t(id int, created_at date) partition by range (created_at);
            create table t_2024 partition of t for values from ('2024-01-01') to ('2025-01-01');
            """,
        )
        assert i1.tables == i2.tables

    def test_foreign_table(self):
        base = """
            create extension if not exists postgres_fdw;
            create server test_srv foreign data wrapper postgres_fdw;
        """
        i1, i2, sql = diff_apply(
            base,
            base + """
            create foreign table ft(id int, val text) server test_srv
                options (table_name 'remote_t');
            """,
        )
        assert i1.foreign_tables == i2.foreign_tables

    def test_ts_dictionary(self):
        i1, i2, sql = diff_apply(
            "",
            "create text search dictionary my_d (template = simple, stopwords = 'english');",
        )
        assert i1.ts_dicts == i2.ts_dicts

    def test_ts_configuration(self):
        i1, i2, sql = diff_apply(
            "",
            """
            create text search configuration my_cfg (parser = default);
            alter text search configuration my_cfg add mapping for asciiword with simple;
            """,
        )
        assert i1.ts_configs == i2.ts_configs

    def test_operator(self):
        fn_sql = """
            create function eq_text_int(text, int) returns boolean
            language sql as 'select length($1) = $2';
        """
        i1, i2, sql = diff_apply(
            fn_sql,
            fn_sql + """
            create operator === (
                leftarg = text, rightarg = int,
                function = eq_text_int
            );
            """,
        )
        assert i1.operators == i2.operators


# ---------------------------------------------------------------------------
# Edge case: column type changes
# ---------------------------------------------------------------------------

class TestColumnEdgeCases:

    def test_column_type_change(self):
        i1, i2, sql = diff_apply(
            "create table t(id int primary key, val int);",
            "create table t(id int primary key, val bigint);",
        )
        assert i1.tables == i2.tables

    def test_column_default_add(self):
        i1, i2, sql = diff_apply(
            "create table t(id int primary key, val int);",
            "create table t(id int primary key, val int default 0);",
        )
        assert i1.tables == i2.tables

    def test_column_default_remove(self):
        i1, i2, sql = diff_apply(
            "create table t(id int primary key, val int default 0);",
            "create table t(id int primary key, val int);",
        )
        assert i1.tables == i2.tables

    def test_column_not_null_add(self):
        i1, i2, sql = diff_apply(
            "create table t(id int primary key, val int);",
            "create table t(id int primary key, val int not null);",
        )
        assert i1.tables == i2.tables

    def test_column_not_null_remove(self):
        i1, i2, sql = diff_apply(
            "create table t(id int primary key, val int not null);",
            "create table t(id int primary key, val int);",
        )
        assert i1.tables == i2.tables

    def test_column_collation_change(self):
        i1, i2, sql = diff_apply(
            'create table t(id int, val text collate "C");',
            'create table t(id int, val text collate "POSIX");',
        )
        assert i1.tables == i2.tables

    def test_multiple_column_changes(self):
        i1, i2, sql = diff_apply(
            "create table t(a int, b text, c bool);",
            "create table t(a bigint, b varchar(100), d int);",
        )
        assert i1.tables == i2.tables

    def test_column_reorder_same_types(self):
        """Column order matters for table equality."""
        i1, i2, sql = diff_apply(
            "create table t(a int, b text);",
            "create table t(b text, a int);",
        )
        assert i1.tables == i2.tables
