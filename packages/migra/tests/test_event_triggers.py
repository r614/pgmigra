from migra.changes import Changes
from migra.db import connect, temporary_database
from migra.schemainspect import get_inspector

SETUP_FUNC = """
CREATE OR REPLACE FUNCTION abort_any_command()
  RETURNS event_trigger
  LANGUAGE plpgsql AS
$$
BEGIN
  RAISE NOTICE 'command % is disabled', tg_tag;
END;
$$;
"""

SETUP_FUNC_2 = """
CREATE OR REPLACE FUNCTION log_ddl_command()
  RETURNS event_trigger
  LANGUAGE plpgsql AS
$$
BEGIN
  RAISE NOTICE 'DDL executed: %', tg_tag;
END;
$$;
"""


def test_event_trigger_basic(db):
    with connect(db) as s:
        s.execute(SETUP_FUNC)
        s.execute(
            "CREATE EVENT TRIGGER abort_ddl ON ddl_command_start EXECUTE FUNCTION abort_any_command();"
        )
        i = get_inspector(s)

        key = '"abort_ddl"'
        assert key in i.event_triggers
        et = i.event_triggers[key]
        assert et.name == "abort_ddl"
        assert et.event == "ddl_command_start"
        assert et.function_name == "abort_any_command"
        assert "CREATE EVENT TRIGGER" in et.create_statement
        assert "ddl_command_start" in et.create_statement


def test_event_trigger_with_tags(db):
    with connect(db) as s:
        s.execute(SETUP_FUNC)
        s.execute("""
            CREATE EVENT TRIGGER no_create_table ON ddl_command_start
              WHEN TAG IN ('CREATE TABLE')
              EXECUTE FUNCTION abort_any_command();
        """)
        i = get_inspector(s)

        key = '"no_create_table"'
        et = i.event_triggers[key]
        assert et.tags is not None
        assert "CREATE TABLE" in et.tags
        assert "WHEN TAG IN" in et.create_statement


def test_event_trigger_disabled(db):
    with connect(db) as s:
        s.execute(SETUP_FUNC)
        s.execute(
            "CREATE EVENT TRIGGER test_et ON ddl_command_start EXECUTE FUNCTION abort_any_command();"
        )
        s.execute("ALTER EVENT TRIGGER test_et DISABLE;")
        i = get_inspector(s)

        et = i.event_triggers['"test_et"']
        assert et.enabled == "D"
        assert "DISABLE" in et.create_statement


def test_event_trigger_diff_add():
    with temporary_database() as d1, temporary_database() as d2:
        with connect(d1) as s1, connect(d2) as s2:
            s1.execute(SETUP_FUNC)
            s2.execute(SETUP_FUNC)
            s2.execute(
                "CREATE EVENT TRIGGER new_et ON ddl_command_start EXECUTE FUNCTION abort_any_command();"
            )
            i_from = get_inspector(s1)
            i_target = get_inspector(s2)

        changes = Changes(i_from, i_target)
        stmts = changes.event_triggers(creations_only=True)
        sql = stmts.sql
        assert "CREATE EVENT TRIGGER" in sql
        assert "new_et" in sql


def test_event_trigger_diff_drop():
    with temporary_database() as d1, temporary_database() as d2:
        with connect(d1) as s1, connect(d2) as s2:
            s1.execute(SETUP_FUNC)
            s2.execute(SETUP_FUNC)
            s1.execute(
                "CREATE EVENT TRIGGER old_et ON ddl_command_start EXECUTE FUNCTION abort_any_command();"
            )
            i_from = get_inspector(s1)
            i_target = get_inspector(s2)

        changes = Changes(i_from, i_target)
        stmts = changes.event_triggers(drops_only=True)
        stmts.safe = False
        sql = stmts.sql
        assert "DROP EVENT TRIGGER" in sql
        assert "old_et" in sql


def test_event_trigger_roundtrip(db):
    with connect(db) as s:
        s.execute(SETUP_FUNC)
        s.execute(
            "CREATE EVENT TRIGGER rt_et ON ddl_command_start EXECUTE FUNCTION abort_any_command();"
        )
        i = get_inspector(s)
        et = i.event_triggers['"rt_et"']
        create_sql = et.create_statement
        drop_sql = et.drop_statement

        s.execute(drop_sql)
        i2 = get_inspector(s)
        assert '"rt_et"' not in i2.event_triggers

        s.execute(create_sql)
        i3 = get_inspector(s)
        et3 = i3.event_triggers['"rt_et"']
        assert et == et3


def test_event_trigger_no_diff_when_equal():
    with temporary_database() as d1, temporary_database() as d2:
        with connect(d1) as s1, connect(d2) as s2:
            s1.execute(SETUP_FUNC)
            s2.execute(SETUP_FUNC)
            s1.execute(
                "CREATE EVENT TRIGGER same_et ON ddl_command_start EXECUTE FUNCTION abort_any_command();"
            )
            s2.execute(
                "CREATE EVENT TRIGGER same_et ON ddl_command_start EXECUTE FUNCTION abort_any_command();"
            )
            i_from = get_inspector(s1)
            i_target = get_inspector(s2)

        changes = Changes(i_from, i_target)
        stmts = changes.event_triggers()
        assert len(stmts) == 0


def test_event_trigger_multiple_tags(db):
    with connect(db) as s:
        s.execute(SETUP_FUNC)
        s.execute("""
            CREATE EVENT TRIGGER multi_tag ON ddl_command_start
              WHEN TAG IN ('CREATE TABLE', 'DROP TABLE')
              EXECUTE FUNCTION abort_any_command();
        """)
        i = get_inspector(s)

        et = i.event_triggers['"multi_tag"']
        assert "CREATE TABLE" in et.tags
        assert "DROP TABLE" in et.tags
