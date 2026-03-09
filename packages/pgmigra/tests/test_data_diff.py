"""Tests for the table data diff feature (pgmigra:data-diff annotation)."""

from __future__ import annotations

import pytest
from pgmigra import Migration
from pgmigra.data_diff import (
    DataDiffAnnotation,
    _compute_table_diff,
    _sql_literal,
    parse_data_diff_annotation,
)
from pgmigra.db import connect, temporary_database


# ---------------------------------------------------------------------------
# Unit tests: annotation parsing
# ---------------------------------------------------------------------------


class TestParseAnnotation:
    def test_no_comment(self):
        assert parse_data_diff_annotation(None) is None
        assert parse_data_diff_annotation("") is None

    def test_no_marker(self):
        assert parse_data_diff_annotation("just a regular comment") is None

    def test_simple_marker(self):
        ann = parse_data_diff_annotation("pgmigra:data-diff")
        assert ann is not None
        assert ann.ignore == frozenset()

    def test_marker_in_longer_comment(self):
        ann = parse_data_diff_annotation(
            "Reference table for countries. pgmigra:data-diff"
        )
        assert ann is not None
        assert ann.ignore == frozenset()

    def test_case_insensitive(self):
        ann = parse_data_diff_annotation("PGMIGRA:DATA-DIFF")
        assert ann is not None

    def test_ignore_single_column(self):
        ann = parse_data_diff_annotation("pgmigra:data-diff(ignore=updated_at)")
        assert ann is not None
        assert ann.ignore == frozenset({"updated_at"})

    def test_ignore_multiple_columns(self):
        ann = parse_data_diff_annotation(
            "pgmigra:data-diff(ignore=updated_at,created_at)"
        )
        assert ann is not None
        assert ann.ignore == frozenset({"updated_at", "created_at"})

    def test_ignore_with_spaces(self):
        ann = parse_data_diff_annotation(
            "pgmigra:data-diff(ignore= updated_at , created_at )"
        )
        assert ann is not None
        assert ann.ignore == frozenset({"updated_at", "created_at"})

    def test_empty_parens(self):
        ann = parse_data_diff_annotation("pgmigra:data-diff()")
        assert ann is not None
        assert ann.ignore == frozenset()

    def test_unknown_options_ignored(self):
        ann = parse_data_diff_annotation("pgmigra:data-diff(foo=bar)")
        assert ann is not None
        assert ann.ignore == frozenset()


# ---------------------------------------------------------------------------
# Unit tests: SQL literal formatting
# ---------------------------------------------------------------------------


class TestSqlLiteral:
    def test_none(self):
        assert _sql_literal(None) == "NULL"

    def test_bool(self):
        assert _sql_literal(True) == "TRUE"
        assert _sql_literal(False) == "FALSE"

    def test_int(self):
        assert _sql_literal(42) == "42"

    def test_float(self):
        assert _sql_literal(3.14) == "3.14"

    def test_string(self):
        assert _sql_literal("hello") == "'hello'"

    def test_string_with_quotes(self):
        assert _sql_literal("it's") == "'it''s'"

    def test_bytes(self):
        assert _sql_literal(b"\xde\xad") == "'\\xdead'::bytea"

    def test_list(self):
        result = _sql_literal([1, 2, 3])
        assert result == "ARRAY[1, 2, 3]"


# ---------------------------------------------------------------------------
# Unit tests: diff computation
# ---------------------------------------------------------------------------


class TestComputeTableDiff:
    def test_inserts_only(self):
        source: list[dict] = []
        target = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        diff = _compute_table_diff(
            source, target, ["id"], ["name"], ["id", "name"], ["name"], "public", "users"
        )
        assert len(diff.inserts) == 2
        assert len(diff.updates) == 0
        assert len(diff.deletes) == 0

    def test_deletes_only(self):
        source = [{"id": 1, "name": "Alice"}]
        target: list[dict] = []
        diff = _compute_table_diff(
            source, target, ["id"], ["name"], ["id", "name"], ["name"], "public", "users"
        )
        assert len(diff.inserts) == 0
        assert len(diff.updates) == 0
        assert len(diff.deletes) == 1

    def test_updates_only(self):
        source = [{"id": 1, "name": "Alice"}]
        target = [{"id": 1, "name": "Alicia"}]
        diff = _compute_table_diff(
            source, target, ["id"], ["name"], ["id", "name"], ["name"], "public", "users"
        )
        assert len(diff.inserts) == 0
        assert len(diff.updates) == 1
        assert len(diff.deletes) == 0

    def test_no_changes(self):
        rows = [{"id": 1, "name": "Alice"}]
        diff = _compute_table_diff(
            rows, rows, ["id"], ["name"], ["id", "name"], ["name"], "public", "users"
        )
        assert len(diff.inserts) == 0
        assert len(diff.updates) == 0
        assert len(diff.deletes) == 0

    def test_ignore_columns(self):
        source = [{"id": 1, "name": "Alice", "updated_at": "2024-01-01"}]
        target = [{"id": 1, "name": "Alice", "updated_at": "2025-01-01"}]
        # compare_columns excludes updated_at
        diff = _compute_table_diff(
            source, target, ["id"], ["name"],
            ["id", "name", "updated_at"], ["name", "updated_at"],
            "public", "users"
        )
        assert len(diff.updates) == 0  # ignored column change

    def test_mixed_operations(self):
        source = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ]
        target = [
            {"id": 1, "name": "Alicia"},
            {"id": 3, "name": "Charlie"},
        ]
        diff = _compute_table_diff(
            source, target, ["id"], ["name"], ["id", "name"], ["name"], "public", "users"
        )
        assert len(diff.inserts) == 1  # Charlie
        assert len(diff.updates) == 1  # Alice → Alicia
        assert len(diff.deletes) == 1  # Bob


# ---------------------------------------------------------------------------
# Unit tests: statement generation
# ---------------------------------------------------------------------------


class TestStatementGeneration:
    def test_upsert_statement(self):
        diff = _compute_table_diff(
            [], [{"id": 1, "name": "Alice"}],
            ["id"], ["name"], ["id", "name"], ["name"], "public", "users"
        )
        stmt = diff._upsert_statement(diff.inserts[0])
        assert 'INSERT INTO "public"."users"' in stmt
        assert "ON CONFLICT" in stmt
        assert "DO UPDATE SET" in stmt
        assert "EXCLUDED" in stmt

    def test_delete_statement(self):
        diff = _compute_table_diff(
            [{"id": 1, "name": "Alice"}], [],
            ["id"], ["name"], ["id", "name"], ["name"], "public", "users"
        )
        stmt = diff._delete_statement(diff.deletes[0])
        assert 'DELETE FROM "public"."users"' in stmt
        assert '"id" = 1' in stmt

    def test_safe_mode_no_deletes(self):
        diff = _compute_table_diff(
            [{"id": 1, "name": "Alice"}], [],
            ["id"], ["name"], ["id", "name"], ["name"], "public", "users"
        )
        stmts = diff.statements(safe=True)
        assert len(stmts) == 0  # no deletes in safe mode

    def test_unsafe_mode_has_deletes(self):
        diff = _compute_table_diff(
            [{"id": 1, "name": "Alice"}], [],
            ["id"], ["name"], ["id", "name"], ["name"], "public", "users"
        )
        stmts = diff.statements(safe=False)
        assert len(stmts) == 1
        assert "DELETE" in stmts[0]

    def test_pk_only_table_uses_do_nothing(self):
        diff = _compute_table_diff(
            [], [{"id": 1}],
            ["id"], [], ["id"], [], "public", "tags"
        )
        stmt = diff._upsert_statement(diff.inserts[0])
        assert "DO NOTHING" in stmt

    def test_null_pk_in_delete(self):
        diff = _compute_table_diff(
            [{"id": None, "name": "ghost"}], [],
            ["id"], ["name"], ["id", "name"], ["name"], "public", "users"
        )
        stmt = diff._delete_statement(diff.deletes[0])
        assert "IS NULL" in stmt


# ---------------------------------------------------------------------------
# Integration tests: full Migration with data diff
# ---------------------------------------------------------------------------


class TestDataDiffIntegration:
    def test_basic_insert_diff(self, db):
        """Tables annotated with pgmigra:data-diff generate upsert statements."""
        with (
            temporary_database() as d0,
            temporary_database() as d1,
        ):
            with connect(d0) as s0, connect(d1) as s1:
                s0.execute("""
                    CREATE TABLE public.countries (
                        code VARCHAR(2) PRIMARY KEY,
                        name TEXT NOT NULL
                    );
                    COMMENT ON TABLE public.countries IS 'pgmigra:data-diff';
                """)
                s1.execute("""
                    CREATE TABLE public.countries (
                        code VARCHAR(2) PRIMARY KEY,
                        name TEXT NOT NULL
                    );
                    COMMENT ON TABLE public.countries IS 'pgmigra:data-diff';
                    INSERT INTO public.countries VALUES ('US', 'United States');
                    INSERT INTO public.countries VALUES ('CA', 'Canada');
                """)

            with connect(d0) as s0, connect(d1) as s1:
                m = Migration(s0, s1)
                m.set_safety(False)
                m.add_all_changes()
                sql = m.sql
                assert "INSERT INTO" in sql
                assert "United States" in sql
                assert "Canada" in sql
                assert "ON CONFLICT" in sql

    def test_update_diff(self, db):
        """Changed rows generate upsert statements."""
        with (
            temporary_database() as d0,
            temporary_database() as d1,
        ):
            with connect(d0) as s0, connect(d1) as s1:
                s0.execute("""
                    CREATE TABLE public.countries (
                        code VARCHAR(2) PRIMARY KEY,
                        name TEXT NOT NULL
                    );
                    COMMENT ON TABLE public.countries IS 'pgmigra:data-diff';
                    INSERT INTO public.countries VALUES ('US', 'United States');
                """)
                s1.execute("""
                    CREATE TABLE public.countries (
                        code VARCHAR(2) PRIMARY KEY,
                        name TEXT NOT NULL
                    );
                    COMMENT ON TABLE public.countries IS 'pgmigra:data-diff';
                    INSERT INTO public.countries VALUES ('US', 'United States of America');
                """)

            with connect(d0) as s0, connect(d1) as s1:
                m = Migration(s0, s1)
                m.set_safety(False)
                m.add_all_changes()
                sql = m.sql
                assert "United States of America" in sql
                assert "ON CONFLICT" in sql

    def test_delete_diff_safe_mode(self, db):
        """In safe mode, DELETE statements are NOT generated."""
        with (
            temporary_database() as d0,
            temporary_database() as d1,
        ):
            with connect(d0) as s0, connect(d1) as s1:
                s0.execute("""
                    CREATE TABLE public.countries (
                        code VARCHAR(2) PRIMARY KEY,
                        name TEXT NOT NULL
                    );
                    COMMENT ON TABLE public.countries IS 'pgmigra:data-diff';
                    INSERT INTO public.countries VALUES ('US', 'United States');
                """)
                s1.execute("""
                    CREATE TABLE public.countries (
                        code VARCHAR(2) PRIMARY KEY,
                        name TEXT NOT NULL
                    );
                    COMMENT ON TABLE public.countries IS 'pgmigra:data-diff';
                """)

            with connect(d0) as s0, connect(d1) as s1:
                m = Migration(s0, s1)
                m.set_safety(True)
                m.add_all_changes()
                sql = m.sql
                assert "DELETE" not in sql

    def test_delete_diff_unsafe_mode(self, db):
        """In unsafe mode, DELETE statements ARE generated."""
        with (
            temporary_database() as d0,
            temporary_database() as d1,
        ):
            with connect(d0) as s0, connect(d1) as s1:
                s0.execute("""
                    CREATE TABLE public.countries (
                        code VARCHAR(2) PRIMARY KEY,
                        name TEXT NOT NULL
                    );
                    COMMENT ON TABLE public.countries IS 'pgmigra:data-diff';
                    INSERT INTO public.countries VALUES ('US', 'United States');
                """)
                s1.execute("""
                    CREATE TABLE public.countries (
                        code VARCHAR(2) PRIMARY KEY,
                        name TEXT NOT NULL
                    );
                    COMMENT ON TABLE public.countries IS 'pgmigra:data-diff';
                """)

            with connect(d0) as s0, connect(d1) as s1:
                m = Migration(s0, s1)
                m.set_safety(False)
                m.add_all_changes()
                sql = m.sql
                assert "DELETE FROM" in sql
                assert "United States" not in sql  # PK-based delete, not by name
                assert "'US'" in sql

    def test_ignore_columns(self, db):
        """Columns in ignore list don't trigger updates."""
        with (
            temporary_database() as d0,
            temporary_database() as d1,
        ):
            with connect(d0) as s0, connect(d1) as s1:
                s0.execute("""
                    CREATE TABLE public.countries (
                        code VARCHAR(2) PRIMARY KEY,
                        name TEXT NOT NULL,
                        updated_at TIMESTAMP DEFAULT now()
                    );
                    COMMENT ON TABLE public.countries IS 'pgmigra:data-diff(ignore=updated_at)';
                    INSERT INTO public.countries VALUES ('US', 'United States', '2024-01-01');
                """)
                s1.execute("""
                    CREATE TABLE public.countries (
                        code VARCHAR(2) PRIMARY KEY,
                        name TEXT NOT NULL,
                        updated_at TIMESTAMP DEFAULT now()
                    );
                    COMMENT ON TABLE public.countries IS 'pgmigra:data-diff(ignore=updated_at)';
                    INSERT INTO public.countries VALUES ('US', 'United States', '2025-01-01');
                """)

            with connect(d0) as s0, connect(d1) as s1:
                m = Migration(s0, s1)
                m.set_safety(False)
                m.add_all_changes()
                # No statements because the only change is in an ignored column
                assert not m.statements

    def test_no_annotation_no_data_diff(self, db):
        """Tables without the annotation do not get data-diffed."""
        with (
            temporary_database() as d0,
            temporary_database() as d1,
        ):
            with connect(d0) as s0, connect(d1) as s1:
                s0.execute("""
                    CREATE TABLE public.countries (
                        code VARCHAR(2) PRIMARY KEY,
                        name TEXT NOT NULL
                    );
                """)
                s1.execute("""
                    CREATE TABLE public.countries (
                        code VARCHAR(2) PRIMARY KEY,
                        name TEXT NOT NULL
                    );
                    INSERT INTO public.countries VALUES ('US', 'United States');
                """)

            with connect(d0) as s0, connect(d1) as s1:
                m = Migration(s0, s1)
                m.set_safety(False)
                m.add_all_changes()
                # No data diff statements (schemas are identical)
                assert not m.statements

    def test_apply_data_diff(self, db):
        """Data diff statements can be applied to synchronize databases."""
        with (
            temporary_database() as d0,
            temporary_database() as d1,
        ):
            with connect(d0) as s0, connect(d1) as s1:
                s0.execute("""
                    CREATE TABLE public.countries (
                        code VARCHAR(2) PRIMARY KEY,
                        name TEXT NOT NULL
                    );
                    COMMENT ON TABLE public.countries IS 'pgmigra:data-diff';
                """)
                s1.execute("""
                    CREATE TABLE public.countries (
                        code VARCHAR(2) PRIMARY KEY,
                        name TEXT NOT NULL
                    );
                    COMMENT ON TABLE public.countries IS 'pgmigra:data-diff';
                    INSERT INTO public.countries VALUES ('US', 'United States');
                    INSERT INTO public.countries VALUES ('CA', 'Canada');
                """)

            with connect(d0) as s0, connect(d1) as s1:
                m = Migration(s0, s1)
                m.set_safety(False)
                m.add_all_changes()
                assert m.statements  # should have data statements
                m.apply()

                # Verify data was synced
                rows = s0.execute(
                    "SELECT code, name FROM public.countries ORDER BY code"
                ).fetchall()
                assert len(rows) == 2
                assert rows[0][0] == "CA"
                assert rows[1][0] == "US"

    def test_fk_ordering(self, db):
        """Tables with FK dependencies get their data in correct order."""
        with (
            temporary_database() as d0,
            temporary_database() as d1,
        ):
            with connect(d0) as s0, connect(d1) as s1:
                s0.execute("""
                    CREATE TABLE public.countries (
                        code VARCHAR(2) PRIMARY KEY,
                        name TEXT NOT NULL
                    );
                    COMMENT ON TABLE public.countries IS 'pgmigra:data-diff';

                    CREATE TABLE public.cities (
                        id SERIAL PRIMARY KEY,
                        name TEXT NOT NULL,
                        country_code VARCHAR(2) REFERENCES public.countries(code)
                    );
                    COMMENT ON TABLE public.cities IS 'pgmigra:data-diff';
                """)
                s1.execute("""
                    CREATE TABLE public.countries (
                        code VARCHAR(2) PRIMARY KEY,
                        name TEXT NOT NULL
                    );
                    COMMENT ON TABLE public.countries IS 'pgmigra:data-diff';

                    CREATE TABLE public.cities (
                        id SERIAL PRIMARY KEY,
                        name TEXT NOT NULL,
                        country_code VARCHAR(2) REFERENCES public.countries(code)
                    );
                    COMMENT ON TABLE public.cities IS 'pgmigra:data-diff';

                    INSERT INTO public.countries VALUES ('US', 'United States');
                    INSERT INTO public.cities (id, name, country_code) VALUES (1, 'New York', 'US');
                """)

            with connect(d0) as s0, connect(d1) as s1:
                m = Migration(s0, s1)
                m.set_safety(False)
                m.add_all_changes()
                sql = m.sql
                # Countries insert must come before cities insert
                countries_pos = sql.index("United States")
                cities_pos = sql.index("New York")
                assert countries_pos < cities_pos

                # Apply and verify
                m.apply()
                rows = s0.execute("SELECT * FROM public.cities").fetchall()
                assert len(rows) == 1

    def test_composite_pk(self, db):
        """Tables with composite primary keys are handled correctly."""
        with (
            temporary_database() as d0,
            temporary_database() as d1,
        ):
            with connect(d0) as s0, connect(d1) as s1:
                s0.execute("""
                    CREATE TABLE public.permissions (
                        role_name TEXT,
                        resource TEXT,
                        level TEXT NOT NULL,
                        PRIMARY KEY (role_name, resource)
                    );
                    COMMENT ON TABLE public.permissions IS 'pgmigra:data-diff';
                """)
                s1.execute("""
                    CREATE TABLE public.permissions (
                        role_name TEXT,
                        resource TEXT,
                        level TEXT NOT NULL,
                        PRIMARY KEY (role_name, resource)
                    );
                    COMMENT ON TABLE public.permissions IS 'pgmigra:data-diff';
                    INSERT INTO public.permissions VALUES ('admin', 'users', 'write');
                    INSERT INTO public.permissions VALUES ('viewer', 'users', 'read');
                """)

            with connect(d0) as s0, connect(d1) as s1:
                m = Migration(s0, s1)
                m.set_safety(False)
                m.add_all_changes()
                sql = m.sql
                assert "INSERT INTO" in sql
                assert "ON CONFLICT" in sql
                assert "admin" in sql
                assert "viewer" in sql

                m.apply()
                rows = s0.execute("SELECT * FROM public.permissions ORDER BY role_name").fetchall()
                assert len(rows) == 2

    def test_no_pk_table_skipped(self, db):
        """Tables without a primary key are skipped (no data diff)."""
        with (
            temporary_database() as d0,
            temporary_database() as d1,
        ):
            with connect(d0) as s0, connect(d1) as s1:
                s0.execute("""
                    CREATE TABLE public.logs (
                        message TEXT
                    );
                    COMMENT ON TABLE public.logs IS 'pgmigra:data-diff';
                """)
                s1.execute("""
                    CREATE TABLE public.logs (
                        message TEXT
                    );
                    COMMENT ON TABLE public.logs IS 'pgmigra:data-diff';
                    INSERT INTO public.logs VALUES ('hello');
                """)

            with connect(d0) as s0, connect(d1) as s1:
                m = Migration(s0, s1)
                m.set_safety(False)
                m.add_all_changes()
                # No PK → no data diff
                assert not m.statements
