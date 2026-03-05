from pgmigra.changes import Changes
from pgmigra.db import connect, temporary_database
from pgmigra.schemainspect import get_inspector

SETUP = """
CREATE TABLE my_table (
    id serial primary key,
    val text
);
"""


def test_rule_do_instead_nothing(db):
    with connect(db) as s:
        s.execute(SETUP)
        s.execute("CREATE RULE no_delete AS ON DELETE TO my_table DO INSTEAD NOTHING;")
        i = get_inspector(s)

        key = '"public"."my_table"."no_delete"'
        assert key in i.rules
        rule = i.rules[key]
        assert rule.name == "no_delete"
        assert rule.schema == "public"
        assert rule.table_name == "my_table"
        assert "NOTHING" in rule.definition


def test_rule_do_also_notify(db):
    with connect(db) as s:
        s.execute(SETUP)
        s.execute(
            "CREATE RULE notify_insert AS ON INSERT TO my_table DO ALSO NOTIFY my_table_changed;"
        )
        i = get_inspector(s)

        key = '"public"."my_table"."notify_insert"'
        assert key in i.rules
        rule = i.rules[key]
        assert "NOTIFY" in rule.definition.upper() or "notify" in rule.definition


def test_rule_conditional(db):
    with connect(db) as s:
        s.execute(SETUP)
        s.execute(
            "CREATE RULE no_empty_insert AS ON INSERT TO my_table WHERE (NEW.val IS NULL) DO INSTEAD NOTHING;"
        )
        i = get_inspector(s)

        key = '"public"."my_table"."no_empty_insert"'
        assert key in i.rules
        rule = i.rules[key]
        assert "WHERE" in rule.definition.upper()


def test_rule_return_excluded(db):
    with connect(db) as s:
        s.execute(SETUP)
        s.execute("CREATE VIEW my_view AS SELECT * FROM my_table;")
        i = get_inspector(s)
        for key in i.rules:
            assert "_RETURN" not in key


def test_rule_diff_add():
    with temporary_database() as d1, temporary_database() as d2:
        with connect(d1) as s1, connect(d2) as s2:
            s1.execute(SETUP)
            s2.execute(SETUP)
            s2.execute(
                "CREATE RULE no_delete AS ON DELETE TO my_table DO INSTEAD NOTHING;"
            )
            i_from = get_inspector(s1)
            i_target = get_inspector(s2)

        changes = Changes(i_from, i_target)
        stmts = changes.rules(creations_only=True)
        sql = stmts.sql
        assert "no_delete" in sql
        assert "CREATE RULE" in sql.upper() or "CREATE OR REPLACE RULE" in sql.upper()


def test_rule_diff_drop():
    with temporary_database() as d1, temporary_database() as d2:
        with connect(d1) as s1, connect(d2) as s2:
            s1.execute(SETUP)
            s2.execute(SETUP)
            s1.execute(
                "CREATE RULE no_delete AS ON DELETE TO my_table DO INSTEAD NOTHING;"
            )
            i_from = get_inspector(s1)
            i_target = get_inspector(s2)

        changes = Changes(i_from, i_target)
        stmts = changes.rules(drops_only=True)
        stmts.safe = False
        sql = stmts.sql
        assert "DROP RULE" in sql
        assert "no_delete" in sql


def test_rule_diff_modify():
    with temporary_database() as d1, temporary_database() as d2:
        with connect(d1) as s1, connect(d2) as s2:
            s1.execute(SETUP)
            s2.execute(SETUP)
            s1.execute("CREATE RULE guard AS ON DELETE TO my_table DO INSTEAD NOTHING;")
            s2.execute(
                "CREATE RULE guard AS ON DELETE TO my_table WHERE (OLD.val IS NOT NULL) DO INSTEAD NOTHING;"
            )
            i_from = get_inspector(s1)
            i_target = get_inspector(s2)

        changes = Changes(i_from, i_target)
        stmts = changes.rules()
        stmts.safe = False
        sql = stmts.sql
        assert "DROP RULE" in sql
        assert "guard" in sql


def test_rule_disabled(db):
    with connect(db) as s:
        s.execute(SETUP)
        s.execute("CREATE RULE no_delete AS ON DELETE TO my_table DO INSTEAD NOTHING;")
        s.execute("ALTER TABLE my_table DISABLE RULE no_delete;")
        i = get_inspector(s)

        key = '"public"."my_table"."no_delete"'
        rule = i.rules[key]
        assert rule.enabled == "D"
        assert "DISABLE RULE" in rule.create_statement


def test_rule_replica(db):
    with connect(db) as s:
        s.execute(SETUP)
        s.execute("CREATE RULE no_delete AS ON DELETE TO my_table DO INSTEAD NOTHING;")
        s.execute("ALTER TABLE my_table ENABLE REPLICA RULE no_delete;")
        i = get_inspector(s)

        key = '"public"."my_table"."no_delete"'
        rule = i.rules[key]
        assert rule.enabled == "R"
        assert "ENABLE REPLICA RULE" in rule.create_statement


def test_rule_always(db):
    with connect(db) as s:
        s.execute(SETUP)
        s.execute("CREATE RULE no_delete AS ON DELETE TO my_table DO INSTEAD NOTHING;")
        s.execute("ALTER TABLE my_table ENABLE ALWAYS RULE no_delete;")
        i = get_inspector(s)

        key = '"public"."my_table"."no_delete"'
        rule = i.rules[key]
        assert rule.enabled == "A"
        assert "ENABLE ALWAYS RULE" in rule.create_statement


def test_rule_roundtrip(db):
    with connect(db) as s:
        s.execute(SETUP)
        s.execute("CREATE RULE no_delete AS ON DELETE TO my_table DO INSTEAD NOTHING;")
        i = get_inspector(s)
        key = '"public"."my_table"."no_delete"'
        rule = i.rules[key]
        create_sql = rule.create_statement
        drop_sql = rule.drop_statement

        s.execute(drop_sql)
        i2 = get_inspector(s)
        assert key not in i2.rules

        s.execute(create_sql)
        i3 = get_inspector(s)
        rule3 = i3.rules[key]
        assert rule == rule3


def test_rule_non_public_schema(db):
    with connect(db) as s:
        s.execute("CREATE SCHEMA other;")
        s.execute("CREATE TABLE other.t (id int primary key, val text);")
        s.execute("CREATE RULE no_delete AS ON DELETE TO other.t DO INSTEAD NOTHING;")
        i = get_inspector(s)

        key = '"other"."t"."no_delete"'
        assert key in i.rules
        rule = i.rules[key]
        assert rule.schema == "other"
        assert rule.table_name == "t"
        assert '"other"' in rule.drop_statement
        assert '"t"' in rule.drop_statement


def test_rule_multiple_on_same_table(db):
    with connect(db) as s:
        s.execute(SETUP)
        s.execute("CREATE RULE no_delete AS ON DELETE TO my_table DO INSTEAD NOTHING;")
        s.execute("CREATE RULE no_update AS ON UPDATE TO my_table DO INSTEAD NOTHING;")
        i = get_inspector(s)

        assert '"public"."my_table"."no_delete"' in i.rules
        assert '"public"."my_table"."no_update"' in i.rules


def test_rule_no_diff_when_equal():
    with temporary_database() as d1, temporary_database() as d2:
        with connect(d1) as s1, connect(d2) as s2:
            s1.execute(SETUP)
            s2.execute(SETUP)
            s1.execute(
                "CREATE RULE no_delete AS ON DELETE TO my_table DO INSTEAD NOTHING;"
            )
            s2.execute(
                "CREATE RULE no_delete AS ON DELETE TO my_table DO INSTEAD NOTHING;"
            )
            i_from = get_inspector(s1)
            i_target = get_inspector(s2)

        changes = Changes(i_from, i_target)
        stmts = changes.rules()
        assert len(stmts) == 0


def test_rule_disabled_roundtrip(db):
    with connect(db) as s:
        s.execute(SETUP)
        s.execute("CREATE RULE no_delete AS ON DELETE TO my_table DO INSTEAD NOTHING;")
        s.execute("ALTER TABLE my_table DISABLE RULE no_delete;")
        i = get_inspector(s)
        key = '"public"."my_table"."no_delete"'
        rule = i.rules[key]
        assert rule.enabled == "D"

        create_sql = rule.create_statement
        drop_sql = rule.drop_statement

        s.execute(drop_sql)
        s.execute(create_sql)
        i2 = get_inspector(s)
        rule2 = i2.rules[key]
        assert rule == rule2
        assert rule2.enabled == "D"
