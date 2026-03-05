from migra.changes import Changes
from migra.db import connect, temporary_database
from migra.schemainspect import get_inspector

SETUP_FUNC = """
CREATE FUNCTION my_eq(a int, b int) RETURNS boolean
  LANGUAGE sql IMMUTABLE AS
$$ SELECT a = b; $$;
"""


def test_operator_basic(db):
    with connect(db) as s:
        s.execute(SETUP_FUNC)
        s.execute("""
            CREATE OPERATOR === (
              FUNCTION = my_eq,
              LEFTARG = int,
              RIGHTARG = int
            );
        """)
        i = get_inspector(s)

        found = [k for k in i.operators if "===" in k]
        assert len(found) == 1
        op = i.operators[found[0]]
        assert op.name == "==="
        assert op.schema == "public"
        assert op.function_name == "my_eq"
        assert "integer" in op.left_type or "int" in op.left_type
        assert "CREATE OPERATOR" in op.create_statement


def test_operator_with_options(db):
    with connect(db) as s:
        s.execute(SETUP_FUNC)
        s.execute("""
            CREATE OPERATOR === (
              FUNCTION = my_eq,
              LEFTARG = int,
              RIGHTARG = int,
              COMMUTATOR = ===,
              HASHES,
              MERGES
            );
        """)
        i = get_inspector(s)

        found = [k for k in i.operators if "===" in k]
        op = i.operators[found[0]]
        assert op.can_hash is True
        assert op.can_merge is True
        assert op.commutator_name == "==="
        assert "HASHES" in op.create_statement
        assert "MERGES" in op.create_statement


def test_operator_roundtrip(db):
    with connect(db) as s:
        s.execute(SETUP_FUNC)
        s.execute("""
            CREATE OPERATOR === (
              FUNCTION = my_eq,
              LEFTARG = int,
              RIGHTARG = int
            );
        """)
        i = get_inspector(s)
        found = [k for k in i.operators if "===" in k]
        op = i.operators[found[0]]
        create_sql = op.create_statement
        drop_sql = op.drop_statement

        s.execute(drop_sql)
        i2 = get_inspector(s)
        found2 = [k for k in i2.operators if "===" in k]
        assert len(found2) == 0

        s.execute(create_sql)
        i3 = get_inspector(s)
        found3 = [k for k in i3.operators if "===" in k]
        assert len(found3) == 1
        op3 = i3.operators[found3[0]]
        assert op == op3


def test_operator_diff_add():
    with temporary_database() as d1, temporary_database() as d2:
        with connect(d1) as s1, connect(d2) as s2:
            s1.execute(SETUP_FUNC)
            s2.execute(SETUP_FUNC)
            s2.execute("""
                CREATE OPERATOR === (
                  FUNCTION = my_eq,
                  LEFTARG = int,
                  RIGHTARG = int
                );
            """)
            i_from = get_inspector(s1)
            i_target = get_inspector(s2)

        changes = Changes(i_from, i_target)
        stmts = changes.operators(creations_only=True)
        sql = stmts.sql
        assert "CREATE OPERATOR" in sql


def test_operator_diff_drop():
    with temporary_database() as d1, temporary_database() as d2:
        with connect(d1) as s1, connect(d2) as s2:
            s1.execute(SETUP_FUNC)
            s2.execute(SETUP_FUNC)
            s1.execute("""
                CREATE OPERATOR === (
                  FUNCTION = my_eq,
                  LEFTARG = int,
                  RIGHTARG = int
                );
            """)
            i_from = get_inspector(s1)
            i_target = get_inspector(s2)

        changes = Changes(i_from, i_target)
        stmts = changes.operators(drops_only=True)
        stmts.safe = False
        sql = stmts.sql
        assert "DROP OPERATOR" in sql


def test_operator_no_diff_when_equal():
    with temporary_database() as d1, temporary_database() as d2:
        with connect(d1) as s1, connect(d2) as s2:
            ddl = (
                SETUP_FUNC
                + """
                CREATE OPERATOR === (
                  FUNCTION = my_eq,
                  LEFTARG = int,
                  RIGHTARG = int
                );
            """
            )
            s1.execute(ddl)
            s2.execute(ddl)
            i_from = get_inspector(s1)
            i_target = get_inspector(s2)

        changes = Changes(i_from, i_target)
        stmts = changes.operators()
        assert len(stmts) == 0


def test_operator_family_basic(db):
    with connect(db) as s:
        s.execute("CREATE OPERATOR FAMILY my_fam USING btree;")
        i = get_inspector(s)

        key = '"public"."my_fam" USING btree'
        assert key in i.operator_families
        f = i.operator_families[key]
        assert f.name == "my_fam"
        assert f.schema == "public"
        assert f.access_method == "btree"
        assert "CREATE OPERATOR FAMILY" in f.create_statement
        assert "USING btree" in f.create_statement


def test_operator_family_roundtrip(db):
    with connect(db) as s:
        s.execute("CREATE OPERATOR FAMILY rt_fam USING hash;")
        i = get_inspector(s)
        key = '"public"."rt_fam" USING hash'
        f = i.operator_families[key]
        create_sql = f.create_statement
        drop_sql = f.drop_statement

        s.execute(drop_sql)
        i2 = get_inspector(s)
        assert key not in i2.operator_families

        s.execute(create_sql)
        i3 = get_inspector(s)
        f3 = i3.operator_families[key]
        assert f == f3


def test_operator_family_diff_add():
    with temporary_database() as d1, temporary_database() as d2:
        with connect(d1) as s1, connect(d2) as s2:
            s2.execute("CREATE OPERATOR FAMILY new_fam USING btree;")
            i_from = get_inspector(s1)
            i_target = get_inspector(s2)

        changes = Changes(i_from, i_target)
        stmts = changes.operator_families(creations_only=True)
        sql = stmts.sql
        assert "CREATE OPERATOR FAMILY" in sql
        assert "new_fam" in sql


def test_operator_family_diff_drop():
    with temporary_database() as d1, temporary_database() as d2:
        with connect(d1) as s1, connect(d2) as s2:
            s1.execute("CREATE OPERATOR FAMILY old_fam USING btree;")
            i_from = get_inspector(s1)
            i_target = get_inspector(s2)

        changes = Changes(i_from, i_target)
        stmts = changes.operator_families(drops_only=True)
        stmts.safe = False
        sql = stmts.sql
        assert "DROP OPERATOR FAMILY" in sql
        assert "old_fam" in sql


def test_operator_family_no_diff_when_equal():
    with temporary_database() as d1, temporary_database() as d2:
        with connect(d1) as s1, connect(d2) as s2:
            s1.execute("CREATE OPERATOR FAMILY same_fam USING btree;")
            s2.execute("CREATE OPERATOR FAMILY same_fam USING btree;")
            i_from = get_inspector(s1)
            i_target = get_inspector(s2)

        changes = Changes(i_from, i_target)
        stmts = changes.operator_families()
        assert len(stmts) == 0


def test_operator_class_basic(db):
    with connect(db) as s:
        s.execute("""
            CREATE TYPE myint;
            CREATE FUNCTION myint_in(cstring) RETURNS myint
              LANGUAGE internal IMMUTABLE STRICT AS 'int4in';
            CREATE FUNCTION myint_out(myint) RETURNS cstring
              LANGUAGE internal IMMUTABLE STRICT AS 'int4out';
            CREATE TYPE myint (
              INPUT = myint_in,
              OUTPUT = myint_out,
              LIKE = int4
            );
            CREATE FUNCTION myint_eq(myint, myint) RETURNS boolean
              LANGUAGE internal IMMUTABLE STRICT AS 'int4eq';
            CREATE FUNCTION myint_lt(myint, myint) RETURNS boolean
              LANGUAGE internal IMMUTABLE STRICT AS 'int4lt';
            CREATE FUNCTION myint_cmp(myint, myint) RETURNS int
              LANGUAGE internal IMMUTABLE STRICT AS 'btint4cmp';
            CREATE OPERATOR = (
              FUNCTION = myint_eq,
              LEFTARG = myint,
              RIGHTARG = myint
            );
            CREATE OPERATOR < (
              FUNCTION = myint_lt,
              LEFTARG = myint,
              RIGHTARG = myint
            );
            CREATE OPERATOR CLASS myint_ops DEFAULT FOR TYPE myint USING btree AS
              OPERATOR 1 <,
              OPERATOR 3 =,
              FUNCTION 1 myint_cmp(myint, myint);
        """)
        i = get_inspector(s)

        key = '"public"."myint_ops" USING btree'
        assert key in i.operator_classes
        oc = i.operator_classes[key]
        assert oc.name == "myint_ops"
        assert oc.access_method == "btree"
        assert oc.is_default is True
        assert len(oc.operators) > 0
        assert len(oc.procs) > 0
        assert "CREATE OPERATOR CLASS" in oc.create_statement
