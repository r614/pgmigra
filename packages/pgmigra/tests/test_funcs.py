from pgmigra.db import connect
from pgmigra.schemainspect import get_inspector

FUNC = """

create or replace function "public"."ordinary_f"(t text)
    returns integer as
    $$select\r\n1$$
    language SQL VOLATILE CALLED ON NULL INPUT SECURITY INVOKER;

"""

PROC = """
CREATE PROCEDURE proc(a integer, b integer)
LANGUAGE SQL
AS $$
select a, b;
$$;


"""


def test_kinds(db):
    with connect(db) as s:
        s.execute(FUNC)

        i = get_inspector(s)
        f = i.functions['"public"."ordinary_f"(t text)']

        assert f.definition == "select\r\n1"
        assert f.kind == "f"

        s.execute(PROC)
        i = get_inspector(s)

        p = i.functions['"public"."proc"(IN a integer, IN b integer)']

        assert p.definition == "\nselect a, b;\n"
        assert p.kind == "p"

        assert (
            p.drop_statement
            == 'drop procedure if exists "public"."proc"(IN a integer, IN b integer);'
        )


def test_long_identifiers(db):
    with connect(db) as s:
        for i in range(50, 70):
            ident = "x" * i
            truncated = "x" * min(i, 63)

            func = FUNC.replace("ordinary_f", ident)

            s.execute(func)

            i = get_inspector(s)

            expected_sig = '"public"."{}"(t text)'.format(truncated)

            f = i.functions[expected_sig]

            assert f.full_definition is not None
