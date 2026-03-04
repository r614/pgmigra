import io
import os
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql

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


@pytest.fixture()
def db(pg_admin):
    dbname = f"test_{uuid4().hex[:12]}"
    pg_admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dbname)))
    yield _pg_url(dbname)
    pg_admin.execute(
        sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(dbname))
    )


def pytest_addoption(parser):
    parser.addoption(
        "--timescale", action="store_true", help="Test with Timescale extension"
    )


def pytest_configure(config):
    config.addinivalue_line("markers", "timescale: mark timescale specific tests")


def pytest_collection_modifyitems(config, items):
    skip_timescale = pytest.mark.skip(reason="need --timescale option to run")
    if not config.getoption("--timescale", default=False):
        for item in items:
            if "timescale" in item.keywords:
                item.add_marker(skip_timescale)
