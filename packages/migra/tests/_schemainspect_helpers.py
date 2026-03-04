import datetime
from copy import deepcopy

from migra.schemainspect import to_pytype
from migra.schemainspect.inspected import ColumnInfo
from migra.schemainspect.misc import quoted_identifier
from migra.schemainspect.pg.objects import InspectedPrivilege
from pytest import raises

T_CREATE = """create table "public"."films" (
    "code" character(5) not null,
    "title" character varying not null,
    "did" bigint not null,
    "date_prod" date,
    "kind" character varying(10),
    "len" interval hour to minute,
    "drange" daterange
);
"""
CV = "character varying"
CV10 = "character varying(10)"
INT = "interval"
INTHM = "interval hour to minute"
TD = datetime.timedelta
FILMS_COLUMNS = dict(
    [
        ("code", ColumnInfo("code", "character", str, dbtypestr="character(5)")),
        ("title", ColumnInfo("title", "character varying", str)),
        ("did", ColumnInfo("did", "bigint", int)),
        ("date_prod", ColumnInfo("date_prod", "date", datetime.date)),
        ("kind", ColumnInfo("kind", CV, str, dbtypestr=CV10)),
        ("len", ColumnInfo("len", INT, TD, dbtypestr=INTHM)),
        ("drange", ColumnInfo("drange", "daterange", str)),
    ]
)
FILMSF_COLUMNS = dict(
    [
        ("title", ColumnInfo("title", "character varying", str)),
        ("release_date", ColumnInfo("release_date", "date", datetime.date)),
    ]
)
d1 = ColumnInfo("d", "date", datetime.date)
d2 = ColumnInfo("def_t", "text", str, default="NULL::text")
d3 = ColumnInfo("def_d", "date", datetime.date, default="'2014-01-01'::date")
FILMSF_INPUTS = [d1, d2, d3]
FDEF = """CREATE OR REPLACE FUNCTION public.films_f(d date, def_t text DEFAULT NULL::text, def_d date DEFAULT '2014-01-01'::date)
 RETURNS TABLE(title character varying, release_date date)
 LANGUAGE sql
AS $function$select 'a'::varchar, '2014-01-01'::date$function$
;"""
VDEF_QUALIFIED = """create or replace view "public"."v_films" as  SELECT films.code,
    films.title,
    films.did,
    films.date_prod,
    films.kind,
    films.len,
    films.drange
   FROM films;
"""
VDEF_UNQUALIFIED = """create or replace view "public"."v_films" as  SELECT code,
    title,
    did,
    date_prod,
    kind,
    len,
    drange
   FROM films;
"""
MVDEF_QUALIFIED = """create materialized view "public"."mv_films" as  SELECT films.code,
    films.title,
    films.did,
    films.date_prod,
    films.kind,
    films.len,
    films.drange
   FROM films;
"""
MVDEF_UNQUALIFIED = """create materialized view "public"."mv_films" as  SELECT code,
    title,
    did,
    date_prod,
    kind,
    len,
    drange
   FROM films;
"""


def setup_pg_schema(s):
    role = s.execute("select 1 from pg_roles where rolname = 'schemainspect_test_role'")
    if not list(role):
        s.execute("create role schemainspect_test_role")
    s.execute("create table emptytable()")
    s.execute("comment on table emptytable is 'emptytable comment'")
    s.execute("create extension pg_trgm")
    s.execute("create schema otherschema")
    s.execute(
        """
        CREATE TABLE films (
            code        char(5) CONSTRAINT firstkey PRIMARY KEY,
            title       varchar NOT NULL,
            did         bigint NOT NULL,
            date_prod   date,
            kind        varchar(10),
            len         interval hour to minute,
            drange      daterange
        );
        grant select, update, delete, insert on table films to schemainspect_test_role;
    """
    )
    s.execute("""CREATE VIEW v_films AS (select * from films)""")
    s.execute("""CREATE VIEW v_films2 AS (select * from v_films)""")
    s.execute(
        """
            CREATE MATERIALIZED VIEW mv_films
            AS (select * from films)
        """
    )
    s.execute(
        """
            CREATE or replace FUNCTION films_f(d date,
            def_t text default null,
            def_d date default '2014-01-01'::date)
            RETURNS TABLE(
                title character varying,
                release_date date
            )
            as $$select 'a'::varchar, '2014-01-01'::date$$
            language sql;
        """
    )
    s.execute("comment on function films_f(date, text, date) is 'films_f comment'")
    s.execute(
        """
        CREATE OR REPLACE FUNCTION inc_f(integer) RETURNS integer AS $$
        BEGIN
                RETURN $1 + 1;
        END;
        $$ LANGUAGE plpgsql stable;
    """
    )
    s.execute(
        """
        CREATE OR REPLACE FUNCTION inc_f_out(integer, out outparam integer) returns integer AS $$
                select 1;
        $$ LANGUAGE sql;
    """
    )
    s.execute(
        """
        CREATE OR REPLACE FUNCTION inc_f_noargs() RETURNS void AS $$
        begin
            perform 1;
        end;
        $$ LANGUAGE plpgsql stable;
    """
    )
    s.execute(
        """
            create index on films(title);
    """
    )
    s.execute(
        """
            create index on mv_films(title);
    """
    )
    s.execute(
        """
            create type ttt as (a int, b text);
    """
    )
    s.execute(
        """
            create type abc as enum ('a', 'b', 'c');
    """
    )
    s.execute(
        """
            create table t_abc (id serial, x abc);
    """
    )


def n(name, schema="public"):
    return quoted_identifier(name, schema=schema)


def asserts_pg(i, has_timescale=False):
    # schemas
    assert list(i.schemas.keys()) == ["otherschema", "public"]
    otherschema = i.schemas["otherschema"]
    assert i.schemas["public"] != i.schemas["otherschema"]
    assert otherschema.create_statement == 'create schema if not exists "otherschema";'
    assert otherschema.drop_statement == 'drop schema if exists "otherschema";'

    # to_pytype
    assert to_pytype(i.dialect, "integer") == int
    assert to_pytype(i.dialect, "nonexistent") == type(None)  # noqa

    # dialect
    assert i.dialect.name == "postgresql"

    # tables and views
    films = n("films")
    v_films = n("v_films")
    v_films2 = n("v_films2")
    v = i.views[v_films]
    public_views = {k: v for k, v in i.views.items() if v.schema == "public"}
    assert list(public_views.keys()) == [v_films, v_films2]
    assert v.columns == FILMS_COLUMNS
    assert v.create_statement in (VDEF_QUALIFIED, VDEF_UNQUALIFIED)
    assert v == v
    assert v == deepcopy(v)
    assert v.drop_statement == f"drop view if exists {v_films};"
    v = i.views[v_films]

    # dependencies
    assert v.dependent_on == [films]
    v = i.views[v_films2]
    assert v.dependent_on == [v_films]

    for k, r in i.relations.items():
        for dependent in r.dependents:
            assert k in i.get_dependency_by_signature(dependent).dependent_on
        for dependency in r.dependent_on:
            assert k in i.get_dependency_by_signature(dependency).dependents

    # materialized views
    mv_films = n("mv_films")
    mv = i.materialized_views[mv_films]
    assert list(i.materialized_views.keys()) == [mv_films]
    assert mv.columns == FILMS_COLUMNS
    assert mv.create_statement in (MVDEF_QUALIFIED, MVDEF_UNQUALIFIED)
    assert mv.drop_statement == f"drop materialized view if exists {mv_films};"

    # materialized view indexes
    assert n("mv_films_title_idx") in mv.indexes

    # functions
    films_f = n("films_f") + "(d date, def_t text, def_d date)"
    inc_f = n("inc_f") + "(integer)"
    inc_f_noargs = n("inc_f_noargs") + "()"
    inc_f_out = n("inc_f_out") + "(integer, OUT outparam integer)"
    public_funcs = [k for k, v in i.functions.items() if v.schema == "public"]
    assert public_funcs == [films_f, inc_f, inc_f_noargs, inc_f_out]
    f = i.functions[films_f]
    f2 = i.functions[inc_f]
    f3 = i.functions[inc_f_noargs]
    f4 = i.functions[inc_f_out]
    assert f == f
    assert f != f2
    assert f.columns == FILMSF_COLUMNS
    assert f.inputs == FILMSF_INPUTS
    assert f3.inputs == []
    assert list(f2.columns.values())[0].name == "inc_f"
    assert list(f3.columns.values())[0].name == "inc_f_noargs"
    assert list(f4.columns.values())[0].name == "outparam"
    fdef = i.functions[films_f].definition
    assert fdef == "select 'a'::varchar, '2014-01-01'::date"
    assert f.create_statement == FDEF
    assert (
        f.drop_statement
        == 'drop function if exists "public"."films_f"(d date, def_t text, def_d date);'
    )
    assert f.comment == "films_f comment"
    assert f2.comment is None

    # extensions
    ext = [
        n("plpgsql", schema="pg_catalog"),
        n("pg_trgm"),
    ]
    if has_timescale:
        ext.append(n("timescaledb"))
    assert [e.quoted_full_name for e in i.extensions.values()] == ext

    # constraints
    cons = i.constraints['"public"."films"."firstkey"']
    assert (
        cons.create_statement
        == 'alter table "public"."films" add constraint "firstkey" PRIMARY KEY using index "firstkey";'
    )

    # tables
    t_films = n("films")
    t = i.tables[t_films]
    empty = i.tables[n("emptytable")]
    assert empty.comment == "emptytable comment"

    # empty tables
    assert empty.columns == {}
    assert (
        empty.create_statement
        == """create table "public"."emptytable" (
);
"""
    )

    # create and drop tables
    assert t.create_statement == T_CREATE
    assert t.drop_statement == f"drop table {t_films};"
    assert t.alter_table_statement("x") == f"alter table {t_films} x;"

    # table indexes
    assert n("films_title_idx") in t.indexes

    # privileges
    g = InspectedPrivilege(
        "table", "public", "films", "select", "schemainspect_test_role"
    )
    g = i.privileges[g.key]
    assert (
        g.create_statement
        == f'grant select on table {t_films} to "schemainspect_test_role";'
    )
    assert (
        g.drop_statement
        == f'revoke select on table {t_films} from "schemainspect_test_role";'
    )

    # composite types
    ct = i.composite_types[n("ttt")]
    assert [(x.name, x.dbtype) for x in ct.columns.values()] == [
        ("a", "integer"),
        ("b", "text"),
    ]
    assert (
        ct.create_statement == 'create type "public"."ttt" as ("a" integer, "b" text);'
    )
    assert ct.drop_statement == 'drop type "public"."ttt";'

    # enums
    assert i.enums[n("abc")].elements == ["a", "b", "c"]
    x = i.tables[n("t_abc")].columns["x"]
    assert x.is_enum
    assert (
        x.change_enum_to_string_statement("t")
        == 'alter table t alter column "x" set data type varchar using "x"::varchar;'
    )
    assert (
        x.change_string_to_enum_statement("t")
        == 'alter table t alter column "x" set data type abc using "x"::abc;'
    )
    tid = i.tables[n("t_abc")].columns["id"]

    with raises(ValueError):
        tid.change_enum_to_string_statement("t")
    with raises(ValueError):
        tid.change_string_to_enum_statement("t")
