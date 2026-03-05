import pytest
from pgmigra.db import connect
from pgmigra.schemainspect import get_inspector


def test_sequences(db):
    with connect(db) as s:
        i = get_inspector(s)

        if i.pg_version < 10:
            pytest.skip("identity columns not supported in 9")

        s.execute(
            """
        create table t(id serial);
        """
        )

        s.execute(
            """
        CREATE SEQUENCE serial START 101;
        """
        )

        s.execute(
            """
        create table t2(id integer generated always as identity);
        """
        )

        i = get_inspector(s)

        seqs = list(i.sequences)

        assert seqs == ['"public"."serial"', '"public"."t_id_seq"']

        unowned = i.sequences['"public"."serial"']
        assert unowned.table_name is None

        owned = i.sequences['"public"."t_id_seq"']
        assert owned.table_name == "t"
        assert owned.quoted_full_table_name == '"public"."t"'
        assert owned.quoted_table_and_column_name == '"public"."t"."id"'
