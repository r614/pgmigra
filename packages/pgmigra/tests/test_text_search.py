from pgmigra.changes import Changes
from pgmigra.db import connect, temporary_database
from pgmigra.schemainspect import get_inspector


def test_ts_dict_basic(db):
    with connect(db) as s:
        s.execute("""
            CREATE TEXT SEARCH DICTIONARY my_dict (
              TEMPLATE = pg_catalog.simple,
              stopwords = 'english'
            );
        """)
        i = get_inspector(s)

        key = '"public"."my_dict"'
        assert key in i.ts_dicts
        d = i.ts_dicts[key]
        assert d.name == "my_dict"
        assert d.schema == "public"
        assert d.template_name == "simple"
        assert d.template_schema == "pg_catalog"
        assert "stopwords" in d.options
        assert "CREATE TEXT SEARCH DICTIONARY" in d.create_statement


def test_ts_dict_roundtrip(db):
    with connect(db) as s:
        s.execute("""
            CREATE TEXT SEARCH DICTIONARY my_dict (
              TEMPLATE = pg_catalog.simple,
              stopwords = 'english'
            );
        """)
        i = get_inspector(s)
        d = i.ts_dicts['"public"."my_dict"']
        create_sql = d.create_statement
        drop_sql = d.drop_statement

        s.execute(drop_sql)
        i2 = get_inspector(s)
        assert '"public"."my_dict"' not in i2.ts_dicts

        s.execute(create_sql)
        i3 = get_inspector(s)
        d3 = i3.ts_dicts['"public"."my_dict"']
        assert d == d3


def test_ts_dict_diff_add():
    with temporary_database() as d1, temporary_database() as d2:
        with connect(d1) as s1, connect(d2) as s2:
            s2.execute("""
                CREATE TEXT SEARCH DICTIONARY new_dict (
                  TEMPLATE = pg_catalog.simple
                );
            """)
            i_from = get_inspector(s1)
            i_target = get_inspector(s2)

        changes = Changes(i_from, i_target)
        stmts = changes.ts_dicts(creations_only=True)
        sql = stmts.sql
        assert "CREATE TEXT SEARCH DICTIONARY" in sql
        assert "new_dict" in sql


def test_ts_dict_diff_drop():
    with temporary_database() as d1, temporary_database() as d2:
        with connect(d1) as s1, connect(d2) as s2:
            s1.execute("""
                CREATE TEXT SEARCH DICTIONARY old_dict (
                  TEMPLATE = pg_catalog.simple
                );
            """)
            i_from = get_inspector(s1)
            i_target = get_inspector(s2)

        changes = Changes(i_from, i_target)
        stmts = changes.ts_dicts(drops_only=True)
        stmts.safe = False
        sql = stmts.sql
        assert "DROP TEXT SEARCH DICTIONARY" in sql
        assert "old_dict" in sql


def test_ts_dict_no_diff_when_equal():
    with temporary_database() as d1, temporary_database() as d2:
        with connect(d1) as s1, connect(d2) as s2:
            ddl = """
                CREATE TEXT SEARCH DICTIONARY same_dict (
                  TEMPLATE = pg_catalog.simple,
                  stopwords = 'english'
                );
            """
            s1.execute(ddl)
            s2.execute(ddl)
            i_from = get_inspector(s1)
            i_target = get_inspector(s2)

        changes = Changes(i_from, i_target)
        stmts = changes.ts_dicts()
        assert len(stmts) == 0


def test_ts_config_basic(db):
    with connect(db) as s:
        s.execute("""
            CREATE TEXT SEARCH CONFIGURATION my_config (PARSER = pg_catalog.default);
            ALTER TEXT SEARCH CONFIGURATION my_config
              ADD MAPPING FOR asciiword WITH simple;
        """)
        i = get_inspector(s)

        key = '"public"."my_config"'
        assert key in i.ts_configs
        c = i.ts_configs[key]
        assert c.name == "my_config"
        assert c.schema == "public"
        assert c.parser_name == "default"
        assert "asciiword" in c.mappings
        assert "CREATE TEXT SEARCH CONFIGURATION" in c.create_statement
        assert "ADD MAPPING FOR asciiword" in c.create_statement


def test_ts_config_roundtrip(db):
    with connect(db) as s:
        s.execute("""
            CREATE TEXT SEARCH CONFIGURATION rt_config (PARSER = pg_catalog.default);
            ALTER TEXT SEARCH CONFIGURATION rt_config
              ADD MAPPING FOR asciiword WITH simple;
        """)
        i = get_inspector(s)
        c = i.ts_configs['"public"."rt_config"']
        create_sql = c.create_statement
        drop_sql = c.drop_statement

        s.execute(drop_sql)
        i2 = get_inspector(s)
        assert '"public"."rt_config"' not in i2.ts_configs

        s.execute(create_sql)
        i3 = get_inspector(s)
        c3 = i3.ts_configs['"public"."rt_config"']
        assert c == c3


def test_ts_config_diff_add():
    with temporary_database() as d1, temporary_database() as d2:
        with connect(d1) as s1, connect(d2) as s2:
            s2.execute("""
                CREATE TEXT SEARCH CONFIGURATION new_config (PARSER = pg_catalog.default);
            """)
            i_from = get_inspector(s1)
            i_target = get_inspector(s2)

        changes = Changes(i_from, i_target)
        stmts = changes.ts_configs(creations_only=True)
        sql = stmts.sql
        assert "CREATE TEXT SEARCH CONFIGURATION" in sql
        assert "new_config" in sql


def test_ts_config_diff_drop():
    with temporary_database() as d1, temporary_database() as d2:
        with connect(d1) as s1, connect(d2) as s2:
            s1.execute("""
                CREATE TEXT SEARCH CONFIGURATION old_config (PARSER = pg_catalog.default);
            """)
            i_from = get_inspector(s1)
            i_target = get_inspector(s2)

        changes = Changes(i_from, i_target)
        stmts = changes.ts_configs(drops_only=True)
        stmts.safe = False
        sql = stmts.sql
        assert "DROP TEXT SEARCH CONFIGURATION" in sql
        assert "old_config" in sql


def test_ts_config_no_diff_when_equal():
    with temporary_database() as d1, temporary_database() as d2:
        with connect(d1) as s1, connect(d2) as s2:
            ddl = """
                CREATE TEXT SEARCH CONFIGURATION same_config (PARSER = pg_catalog.default);
                ALTER TEXT SEARCH CONFIGURATION same_config
                  ADD MAPPING FOR asciiword WITH simple;
            """
            s1.execute(ddl)
            s2.execute(ddl)
            i_from = get_inspector(s1)
            i_target = get_inspector(s2)

        changes = Changes(i_from, i_target)
        stmts = changes.ts_configs()
        assert len(stmts) == 0


def test_ts_config_multiple_mappings(db):
    with connect(db) as s:
        s.execute("""
            CREATE TEXT SEARCH CONFIGURATION multi_config (PARSER = pg_catalog.default);
            ALTER TEXT SEARCH CONFIGURATION multi_config
              ADD MAPPING FOR asciiword WITH simple;
            ALTER TEXT SEARCH CONFIGURATION multi_config
              ADD MAPPING FOR word WITH simple;
        """)
        i = get_inspector(s)

        c = i.ts_configs['"public"."multi_config"']
        assert "asciiword" in c.mappings
        assert "word" in c.mappings


def test_ts_dict_no_options(db):
    with connect(db) as s:
        s.execute("""
            CREATE TEXT SEARCH DICTIONARY minimal_dict (
              TEMPLATE = pg_catalog.simple
            );
        """)
        i = get_inspector(s)

        d = i.ts_dicts['"public"."minimal_dict"']
        assert d.options is None or d.options == ""
        assert "CREATE TEXT SEARCH DICTIONARY" in d.create_statement
