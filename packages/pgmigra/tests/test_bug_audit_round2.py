"""Tests for bugs found in the second-round deep code audit.

Each test confirms a suspected bug by constructing the exact scenario.
"""


from pgmigra.db import connect, temporary_database
from pgmigra.schemainspect import get_inspector
from pgmigra.schemainspect.pg.objects.cast import InspectedCast
from pgmigra.schemainspect.pg.objects.collation import InspectedCollation
from pgmigra.schemainspect.pg.objects.event_trigger import InspectedEventTrigger
from pgmigra.schemainspect.pg.objects.extension import InspectedExtension
from pgmigra.schemainspect.pg.objects.fdw import InspectedFDW
from pgmigra.schemainspect.pg.objects.foreign_server import InspectedForeignServer
from pgmigra.schemainspect.pg.objects.index import InspectedIndex
from pgmigra.schemainspect.pg.objects.operator import InspectedOperator

# -- BUG 7: InspectedCast function_schema missing from __eq__ ----------------


def test_cast_function_schema_not_compared():
    """Two casts with same function name but different schemas should differ."""
    c1 = InspectedCast(
        source_type="text",
        target_type="integer",
        context="e",
        method="f",
        function_name="my_cast",
        function_schema="public",
        function_args="text",
    )
    c2 = InspectedCast(
        source_type="text",
        target_type="integer",
        context="e",
        method="f",
        function_name="my_cast",
        function_schema="other_schema",
        function_args="text",
    )
    assert c1 != c2, (
        "BUG: casts with different function_schema compare as equal"
    )


# -- BUG 8: InspectedOperator function_schema missing from __eq__ ------------


def test_operator_function_schema_not_compared():
    """Two operators with same function name but different schemas should differ."""
    o1 = InspectedOperator(
        name="##",
        schema="public",
        left_type="integer",
        right_type="integer",
        result_type="integer",
        function_name="my_op_func",
        function_schema="public",
        function_args="integer, integer",
        commutator_name=None,
        commutator_schema=None,
        negator_name=None,
        negator_schema=None,
        can_hash=False,
        can_merge=False,
    )
    o2 = InspectedOperator(
        name="##",
        schema="public",
        left_type="integer",
        right_type="integer",
        result_type="integer",
        function_name="my_op_func",
        function_schema="other_schema",
        function_args="integer, integer",
        commutator_name=None,
        commutator_schema=None,
        negator_name=None,
        negator_schema=None,
        can_hash=False,
        can_merge=False,
    )
    assert o1 != o2, (
        "BUG: operators with different function_schema compare as equal"
    )


# -- BUG 9: InspectedFunction misses PARALLEL SAFE / COST / LEAKPROOF -------


def test_function_parallel_safe_not_detected():
    """Changing PARALLEL SAFE should be detected as a difference."""
    with temporary_database() as d1, temporary_database() as d2:
        with connect(d1) as s1, connect(d2) as s2:
            s1.execute("""
                create function add_nums(a int, b int) returns int
                language sql immutable parallel unsafe
                as $$ select a + b $$;
            """)
            s2.execute("""
                create function add_nums(a int, b int) returns int
                language sql immutable parallel safe
                as $$ select a + b $$;
            """)
            i1 = get_inspector(s1)
            i2 = get_inspector(s2)

        key = '"public"."add_nums"(a integer, b integer)'
        f1 = i1.functions[key]
        f2 = i2.functions[key]
        assert f1 != f2, (
            "BUG: functions differing only in PARALLEL SAFE/UNSAFE compare as equal"
        )


def test_function_cost_not_detected():
    """Changing COST should be detected as a difference."""
    with temporary_database() as d1, temporary_database() as d2:
        with connect(d1) as s1, connect(d2) as s2:
            s1.execute("""
                create function cost_test(x int) returns int
                language sql cost 100
                as $$ select x $$;
            """)
            s2.execute("""
                create function cost_test(x int) returns int
                language sql cost 1000
                as $$ select x $$;
            """)
            i1 = get_inspector(s1)
            i2 = get_inspector(s2)

        key = '"public"."cost_test"(x integer)'
        f1 = i1.functions[key]
        f2 = i2.functions[key]
        assert f1 != f2, (
            "BUG: functions differing only in COST compare as equal"
        )


def test_function_leakproof_not_detected():
    """Changing LEAKPROOF should be detected as a difference."""
    with temporary_database() as d1, temporary_database() as d2:
        with connect(d1) as s1, connect(d2) as s2:
            s1.execute("""
                create function leak_test(x int) returns int
                language sql
                as $$ select x $$;
            """)
            s2.execute("""
                create function leak_test(x int) returns int
                language sql leakproof
                as $$ select x $$;
            """)
            i1 = get_inspector(s1)
            i2 = get_inspector(s2)

        key = '"public"."leak_test"(x integer)'
        f1 = i1.functions[key]
        f2 = i2.functions[key]
        assert f1 != f2, (
            "BUG: functions differing only in LEAKPROOF compare as equal"
        )


# -- BUG 10: InspectedEventTrigger owner not compared -----------------------


def test_event_trigger_owner_not_compared():
    """Event triggers with different owners should not compare as equal."""
    et1 = InspectedEventTrigger(
        name="my_trigger",
        owner="alice",
        event="ddl_command_end",
        enabled="O",
        tags=None,
        function_name="my_func",
        function_schema="public",
    )
    et2 = InspectedEventTrigger(
        name="my_trigger",
        owner="bob",
        event="ddl_command_end",
        enabled="O",
        tags=None,
        function_name="my_func",
        function_schema="public",
    )
    assert et1 != et2, (
        "BUG: event triggers with different owners compare as equal"
    )


# -- BUG 11: InspectedFDW owner not compared --------------------------------


def test_fdw_owner_not_compared():
    """FDWs with different owners should not compare as equal."""
    f1 = InspectedFDW(
        name="my_fdw",
        owner="alice",
        handler_name=None,
        handler_schema=None,
        validator_name=None,
        validator_schema=None,
        options=None,
    )
    f2 = InspectedFDW(
        name="my_fdw",
        owner="bob",
        handler_name=None,
        handler_schema=None,
        validator_name=None,
        validator_schema=None,
        options=None,
    )
    assert f1 != f2, (
        "BUG: FDWs with different owners compare as equal"
    )


# -- BUG 12: InspectedForeignServer owner not compared ----------------------


def test_foreign_server_owner_not_compared():
    """Foreign servers with different owners should not compare as equal."""
    s1 = InspectedForeignServer(
        name="my_server",
        fdw_name="my_fdw",
        owner="alice",
        server_type=None,
        server_version=None,
        options=None,
    )
    s2 = InspectedForeignServer(
        name="my_server",
        fdw_name="my_fdw",
        owner="bob",
        server_type=None,
        server_version=None,
        options=None,
    )
    assert s1 != s2, (
        "BUG: foreign servers with different owners compare as equal"
    )


# -- BUG 13: InspectedIndex key_collations not compared ----------------------


def test_index_key_collations_not_compared():
    """Indexes with different key_collations should not compare as equal."""
    i1 = InspectedIndex(
        name="idx",
        schema="public",
        table_name="t",
        key_columns=["name"],
        key_options=["0"],
        num_att=1,
        is_unique=False,
        is_pk=False,
        is_exclusion=False,
        is_immediate=True,
        is_clustered=False,
        key_collations=["0"],
        key_expressions=None,
        partial_predicate=None,
        algorithm="btree",
        definition="CREATE INDEX idx ON public.t USING btree (name)",
    )
    i2 = InspectedIndex(
        name="idx",
        schema="public",
        table_name="t",
        key_columns=["name"],
        key_options=["0"],
        num_att=1,
        is_unique=False,
        is_pk=False,
        is_exclusion=False,
        is_immediate=True,
        is_clustered=False,
        key_collations=["12345"],
        key_expressions=None,
        partial_predicate=None,
        algorithm="btree",
        definition='CREATE INDEX idx ON public.t USING btree (name COLLATE "C")',
    )
    assert i1 != i2, (
        "BUG: indexes with different key_collations compare as equal"
    )


# -- BUG 14: InspectedCollation lc_ctype not compared -----------------------


def test_collation_lc_ctype_not_compared():
    """Collations with different lc_ctype should not compare as equal."""
    c1 = InspectedCollation(
        name="my_coll",
        schema="public",
        provider="libc",
        encoding=None,
        lc_collate="en_US.UTF-8",
        lc_ctype="en_US.UTF-8",
        version=None,
    )
    c2 = InspectedCollation(
        name="my_coll",
        schema="public",
        provider="libc",
        encoding=None,
        lc_collate="en_US.UTF-8",
        lc_ctype="C",
        version=None,
    )
    assert c1 != c2, (
        "BUG: collations with different lc_ctype compare as equal"
    )


# -- BUG 15: InspectedExtension schema change not handled by alter_statements


def test_extension_schema_change_emits_nothing():
    """Extension schema change should produce ALTER EXTENSION SET SCHEMA."""
    e_from = InspectedExtension(name="pgcrypto", schema="public", version="1.3")
    e_target = InspectedExtension(name="pgcrypto", schema="extensions", version="1.3")
    assert e_from != e_target
    stmts = e_target.alter_statements(e_from)
    has_set_schema = any("set schema" in s.lower() for s in stmts if s)
    assert has_set_schema, (
        f"BUG: extension schema change produces no SET SCHEMA. Got: {stmts}"
    )
