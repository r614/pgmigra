from migra.db import _pg_url, connect, temporary_database


def test_pg_url_formats():
    """Verify _pg_url handles various auth configurations."""
    url = _pg_url("localhost", "5432", "", "testdb")
    assert url == "postgresql://localhost:5432/testdb"

    url = _pg_url("localhost", "5432", "myuser", "testdb")
    assert url == "postgresql://myuser@localhost:5432/testdb"

    url = _pg_url("localhost", "5432", "myuser", "testdb", "secret")
    assert url == "postgresql://myuser:secret@localhost:5432/testdb"

    url = _pg_url("localhost", "5432", "myuser", "testdb", "")
    assert url == "postgresql://myuser@localhost:5432/testdb"


def test_temporary_database_works():
    """Verify temporary_database creates and cleans up databases."""
    with temporary_database() as url:
        assert "postgresql://" in url
        with connect(url) as conn:
            result = conn.execute("SELECT 1 as x")
            assert list(result)[0].x == 1
