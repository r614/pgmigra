from pgmigra import Migration
from pgmigra.db import connect, temporary_database


def test_aggregate_functions():
    """Verify aggregate functions (relationtype 'a') are not silently ignored."""
    with temporary_database() as d0, temporary_database() as d1:
        with connect(d1) as s1:
            s1.execute("""
                CREATE FUNCTION array_concat_agg_transfn(anycompatiblearray, anycompatiblearray)
                RETURNS anycompatiblearray LANGUAGE sql IMMUTABLE AS
                'SELECT array_cat($1, $2)';
            """)
            s1.execute("""
                CREATE AGGREGATE array_concat_agg(anycompatiblearray) (
                    SFUNC = array_concat_agg_transfn,
                    STYPE = anycompatiblearray,
                    INITCOND = '{}'
                );
            """)

        with connect(d0) as s0, connect(d1) as s1:
            m = Migration(s0, s1)
            m.set_safety(False)
            m.add_all_changes()

            sql_out = m.sql
            assert "array_concat_agg" in sql_out, (
                f"Aggregate function not found in migration SQL. Got:\n{sql_out}"
            )
