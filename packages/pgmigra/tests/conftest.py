import io
import os

import pgmigra.db
import psycopg
import pytest

schemainspect_test_role = "schemainspect_test_role"


def create_role(s, rolename):
    role = s.execute(
        "SELECT 1 FROM pg_roles WHERE rolname = %s",
        (rolename,),
    )

    role_exists = bool(list(role))

    if not role_exists:
        s.execute(f"create role {rolename}")


def outs():
    return io.StringIO(), io.StringIO()


def _pg_url(dbname: str = "postgres") -> str:
    host = os.environ.get("PGHOST", "localhost")
    port = os.environ.get("PGPORT", "5432")
    user = os.environ.get("PGUSER", "")
    userspec = f"{user}@" if user else ""
    return f"postgresql://{userspec}{host}:{port}/{dbname}"


@pytest.fixture(scope="session")
def pg_admin():
    conn = psycopg.connect(_pg_url("postgres"), autocommit=True)
    yield conn
    conn.close()


@pytest.fixture(scope="session", autouse=True)
def _db_pool():
    pool = pgmigra.db.DatabasePool(_pg_url("postgres"))
    pgmigra.db._database_pool = pool
    yield pool
    pgmigra.db._database_pool = None
    pool.cleanup()


@pytest.fixture()
def db():
    with pgmigra.db.temporary_database() as url:
        yield url


@pytest.fixture(scope="session")
def pg_version():
    conn = psycopg.connect(_pg_url("postgres"))
    version = conn.info.server_version // 10000
    conn.close()
    return version


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "requires_pg: mark tests requiring a minimum PG version"
    )


def pytest_runtest_setup(item):
    for marker in item.iter_markers("requires_pg"):
        min_version = marker.kwargs.get("min_version", 14)
        if not hasattr(item.config, "_pg_version"):
            conn = psycopg.connect(_pg_url("postgres"))
            item.config._pg_version = conn.info.server_version // 10000
            conn.close()
        if item.config._pg_version < min_version:
            pytest.skip(
                f"Requires PG {min_version}+, running {item.config._pg_version}"
            )
