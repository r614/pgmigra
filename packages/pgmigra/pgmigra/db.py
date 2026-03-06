from __future__ import annotations

import os
import threading
from collections import deque
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any
from uuid import uuid4

import psycopg
from psycopg import sql
from psycopg.rows import namedtuple_row

_RESET_SQL = """
DO $$ DECLARE r RECORD; BEGIN
  FOR r IN SELECT evtname FROM pg_event_trigger LOOP
    EXECUTE format('DROP EVENT TRIGGER IF EXISTS %I', r.evtname);
  END LOOP;
  FOR r IN SELECT nspname FROM pg_namespace
    WHERE nspname NOT IN ('pg_catalog','information_schema','pg_toast')
    AND nspname NOT LIKE 'pg_temp%%' AND nspname NOT LIKE 'pg_toast_temp%%'
  LOOP
    EXECUTE format('DROP SCHEMA IF EXISTS %I CASCADE', r.nspname);
  END LOOP;
  CREATE SCHEMA public;
  FOR r IN SELECT pubname FROM pg_publication LOOP
    EXECUTE format('DROP PUBLICATION IF EXISTS %I', r.pubname);
  END LOOP;
  FOR r IN SELECT srvname FROM pg_foreign_server LOOP
    EXECUTE format('DROP SERVER IF EXISTS %I CASCADE', r.srvname);
  END LOOP;
  FOR r IN SELECT fdwname FROM pg_foreign_data_wrapper WHERE oid >= 16384 LOOP
    EXECUTE format('DROP FOREIGN DATA WRAPPER IF EXISTS %I CASCADE', r.fdwname);
  END LOOP;
  FOR r IN SELECT extname FROM pg_extension WHERE extname != 'plpgsql' LOOP
    EXECUTE format('DROP EXTENSION IF EXISTS %I CASCADE', r.extname);
  END LOOP;
END $$;
"""


class DatabasePool:
    """Recycles test databases via schema reset instead of CREATE/DROP DATABASE."""

    def __init__(self, admin_url: str) -> None:
        self._admin_url = admin_url
        self._admin_conn = psycopg.connect(admin_url, autocommit=True)
        self._available: deque[str] = deque()
        self._all_dbs: list[str] = []
        self._lock = threading.Lock()
        self._system_roles: set[str] = {
            r[0]
            for r in self._admin_conn.execute("SELECT rolname FROM pg_roles").fetchall()
        }

    @contextmanager
    def checkout(self) -> Generator[str, None, None]:
        url = self._get_or_create()
        try:
            yield url
        finally:
            self._reset_and_return(url)

    def _get_or_create(self) -> str:
        with self._lock:
            if self._available:
                return self._available.pop()
        dbname = f"pool_{uuid4().hex[:12]}"
        self._admin_conn.execute(
            sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dbname))
        )
        url = self._admin_url.rsplit("/", 1)[0] + "/" + dbname
        with self._lock:
            self._all_dbs.append(dbname)
        return url

    def _reset_and_return(self, url: str) -> None:
        dbname = url.rsplit("/", 1)[-1]
        try:
            with psycopg.connect(url, autocommit=True) as conn:
                conn.execute(_RESET_SQL)
            with self._lock:
                self._available.append(url)
        except Exception:
            try:
                self._admin_conn.execute(
                    sql.SQL("DROP DATABASE IF EXISTS {}").format(
                        sql.Identifier(dbname)
                    )
                )
            except Exception:
                pass
            with self._lock:
                if dbname in self._all_dbs:
                    self._all_dbs.remove(dbname)

    def _cleanup_roles(self) -> None:
        current = {
            r[0]
            for r in self._admin_conn.execute("SELECT rolname FROM pg_roles").fetchall()
        }
        for role in current - self._system_roles:
            try:
                self._admin_conn.execute(
                    sql.SQL("DROP OWNED BY {} CASCADE").format(sql.Identifier(role))
                )
                self._admin_conn.execute(
                    sql.SQL("DROP ROLE IF EXISTS {}").format(sql.Identifier(role))
                )
            except Exception:
                pass

    def cleanup(self) -> None:
        for dbname in self._all_dbs:
            try:
                self._admin_conn.execute(
                    sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(
                        sql.Identifier(dbname)
                    )
                )
            except Exception:
                pass
        self._admin_conn.close()


_database_pool: DatabasePool | None = None


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
    if _database_pool is not None:
        with _database_pool.checkout() as url:
            yield url
        return
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
