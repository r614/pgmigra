from __future__ import annotations

import os
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any
from uuid import uuid4

import psycopg
from psycopg import sql
from psycopg.rows import namedtuple_row


def _pg_url(host: str, port: str, user: str, dbname: str, password: str = "") -> str:
    userspec = ""
    if user:
        if password:
            userspec = f"{user}:{password}@"
        else:
            userspec = f"{user}@"
    return f"postgresql://{userspec}{host}:{port}/{dbname}"


@contextmanager
def connect(url: str) -> Generator[psycopg.Connection[Any], None, None]:
    with psycopg.connect(url, row_factory=namedtuple_row, autocommit=True) as conn:
        yield conn


def execute(conn: psycopg.Connection[Any], sql: str) -> None:
    conn.execute(sql)  # type: ignore[no-matching-overload]  # dynamic SQL


def load_sql_from_file(conn: psycopg.Connection[Any], path: str) -> None:
    with open(path) as f:
        conn.execute(sql.SQL(f.read()))  # type: ignore[invalid-argument-type]  # trusted SQL files


@contextmanager
def temporary_database(host: str = "localhost") -> Generator[str, None, None]:
    host = os.environ.get("PGHOST", host)
    port = os.environ.get("PGPORT", "5432")
    user = os.environ.get("PGUSER", "")
    password = os.environ.get("PGPASSWORD", "")
    dbname = f"test_{uuid4().hex[:12]}"
    admin_url = _pg_url(host, port, user, "postgres", password)
    with psycopg.connect(admin_url, autocommit=True) as admin_conn:
        admin_conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dbname)))
    url = _pg_url(host, port, user, dbname, password)
    try:
        yield url
    finally:
        with psycopg.connect(admin_url, autocommit=True) as admin_conn:
            admin_conn.execute(
                sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(dbname))
            )
