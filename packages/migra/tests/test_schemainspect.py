from copy import deepcopy

from migra import schemainspect
from migra.schemainspect.inspected import ColumnInfo
from migra.schemainspect.pg.objects import (
    InspectedConstraint,
    InspectedEnum,
    InspectedExtension,
    InspectedIndex,
    InspectedSequence,
)
from pytest import raises


def test_basic_schemainspect():
    a = ColumnInfo("a", "text", str)
    a2 = ColumnInfo("a", "text", str)
    b = ColumnInfo("b", "varchar", str, dbtypestr="varchar(10)")
    b2 = ColumnInfo(
        "b", "text", str, dbtypestr="text", default="'d'::text", not_null=True
    )
    assert a == a2
    assert a == a
    assert a != b
    assert b != b2
    alter = b2.alter_table_statements(b, "t")
    assert alter == [
        "alter table t alter column \"b\" set default 'd'::text;",
        'alter table t alter column "b" set not null;',
        'alter table t alter column "b" set data type text using "b"::text;',
    ]
    alter = b.alter_table_statements(b2, "t")
    assert alter == [
        'alter table t alter column "b" drop default;',
        'alter table t alter column "b" drop not null;',
        'alter table t alter column "b" set data type varchar(10) using "b"::varchar(10);',
    ]
    b.add_column_clause == 'add column "b"'
    b.drop_column_clause == 'drop column "b"'


def test_inspected():
    x = schemainspect.Inspected()
    x.name = "b"
    x.schema = "a"
    assert x.quoted_full_name == '"a"."b"'
    assert x.unquoted_full_name == "a.b"
    x = schemainspect.ColumnInfo(name="a", dbtype="integer", pytype=int)
    assert x.creation_clause == '"a" integer'
    x.default = "5"
    x.not_null = True
    assert x.creation_clause == '"a" integer not null default 5'


def test_postgres_objects():
    ex = InspectedExtension("name", "schema", "1.2")
    assert ex.drop_statement == 'drop extension if exists "name";'
    assert (
        ex.create_statement
        == 'create extension if not exists "name" with schema "schema" version \'1.2\';'
    )
    assert ex.update_statement == "alter extension \"name\" update to '1.2';"
    ex2 = deepcopy(ex)
    assert ex == ex2
    ex2.version = "2.1"
    assert ex != ex2

    ex3 = ex2.unversioned_copy()
    assert ex2 != ex3

    assert ex3.update_statement is None

    assert ex3.drop_statement == 'drop extension if exists "name";'
    assert (
        ex3.create_statement
        == 'create extension if not exists "name" with schema "schema";'
    )

    ix = InspectedIndex(
        name="name",
        schema="schema",
        table_name="table",
        key_columns=["y"],
        index_columns=["y"],
        included_columns=[],
        key_options="0",
        num_att="1",
        is_unique=False,
        is_pk=True,
        is_exclusion=False,
        is_immediate=True,
        is_clustered=False,
        key_collations="0",
        key_expressions=None,
        partial_predicate=None,
        algorithm="BRIN",
        definition="create index name on t(x)",
    )
    assert ix.drop_statement == 'drop index if exists "schema"."name";'
    assert ix.create_statement == "create index name on t(x);"
    ix2 = deepcopy(ix)
    assert ix == ix2
    ix2.table_name = "table2"
    assert ix != ix2
    i = InspectedSequence("name", "schema")
    assert i.create_statement == 'create sequence "schema"."name";'
    assert i.drop_statement == 'drop sequence if exists "schema"."name";'
    i2 = deepcopy(i)
    assert i == i2
    i2.schema = "schema2"
    assert i != i2
    i = InspectedEnum("name", "schema", ["a", "b", "c"])
    assert (
        i.create_statement == "create type \"schema\".\"name\" as enum ('a', 'b', 'c');"
    )
    assert i.drop_statement == 'drop type "schema"."name";'
    i2 = InspectedEnum("name", "schema", ["a", "a1", "c", "d"])
    assert i.can_be_changed_to(i)
    assert i != i2
    assert not i.can_be_changed_to(i2)
    i2.elements = ["a", "b"]
    assert i2.can_be_changed_to(i)
    i2.elements = ["b", "a"]
    assert not i2.can_be_changed_to(i)
    i2.elements = ["a", "b", "c"]
    assert i2.can_be_changed_to(i)
    assert i.can_be_changed_to(i2)
    i2.elements = ["a", "a1", "c", "d", "b"]
    assert not i.can_be_changed_to(i2)
    with raises(ValueError):
        i.change_statements(i2)
    i2.elements = ["a0", "a", "a1", "b", "c", "d"]
    assert i.can_be_changed_to(i2)
    assert i.change_statements(i2) == [
        "alter type \"schema\".\"name\" add value 'a0' before 'a';",
        "alter type \"schema\".\"name\" add value 'a1' after 'a';",
        "alter type \"schema\".\"name\" add value 'd' after 'c';",
    ]
    c = InspectedConstraint(
        constraint_type="PRIMARY KEY",
        definition="PRIMARY KEY (code)",
        index="firstkey",
        name="firstkey",
        schema="public",
        table_name="films",
    )
    assert (
        c.create_statement
        == 'alter table "public"."films" add constraint "firstkey" PRIMARY KEY using index "firstkey";'
    )
    c2 = deepcopy(c)
    assert c == c2
    c.index = None
    assert c != c2
    assert (
        c.create_statement
        == 'alter table "public"."films" add constraint "firstkey" PRIMARY KEY (code);'
    )
    assert (
        c.drop_statement == 'alter table "public"."films" drop constraint "firstkey";'
    )
