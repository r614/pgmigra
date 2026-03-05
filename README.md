# pgmigra

[![Test](https://github.com/r614/migra/actions/workflows/test.yml/badge.svg)](https://github.com/r614/migra/actions/workflows/test.yml)

Like `diff` but for PostgreSQL schemas. Compares two PostgreSQL databases and outputs the DDL statements needed to make them match.

```console
$ pgmigra postgresql:///a postgresql:///b
alter table "public"."products" add column newcolumn text;

alter table "public"."products" add constraint "x" CHECK ((price > (0)::numeric));
```

pgmigra is a fork of the now-deprecated [djrobstep/migra](https://github.com/djrobstep/migra) and
[djrobstep/schemainspect](https://github.com/djrobstep/schemainspect). Compared to the original:

- Actively maintained with PG 14-18 support (drops legacy PG 9-13)
- Centralized object registry with dependency-aware DDL ordering
- Many more DDL object types (foreign data wrappers, text search, operators, publications, etc.)
- Combined schemainspect into a single package
- Modern Python (3.10+, type hints, psycopg3)

## Installation

Install from GitHub with pip:

```bash
pip install "pgmigra @ git+https://github.com/r614/migra.git#subdirectory=packages/pgmigra"
```

Or with [uv](https://github.com/astral-sh/uv):

```bash
uv add "pgmigra @ git+https://github.com/r614/migra.git#subdirectory=packages/pgmigra"
```

## Usage

### Schema diffing

```console
$ pgmigra postgresql:///source postgresql:///target
create table "public"."users" (
    "id" serial primary key,
    "email" text not null
);
```

Empty output means the schemas already match.

### Safe migrations

By default, pgmigra exits with an error if any `DROP` statements would be generated. Use `--unsafe`
to allow destructive changes:

```console
$ pgmigra --unsafe postgresql:///source postgresql:///target
drop table "public"."old_table";
```

### Generating migration scripts

Pipe the output to a file, review it, then apply in a transaction:

```console
$ pgmigra --unsafe postgresql:///production postgresql:///target > migration.sql
$ psql postgresql:///production -1 -f migration.sql
```

### Schema creation from scratch

Use `EMPTY` as the source to generate full schema creation SQL:

```console
$ pgmigra --unsafe EMPTY postgresql:///mydb
```

### Privileges and roles

Include `GRANT`/`REVOKE` statements and role diffing with opt-in flags:

```console
$ pgmigra --with-privileges --with-roles postgresql:///a postgresql:///b
```

### Python API

```python
from pgmigra import Migration
from pgmigra.db import connect

with connect("postgresql:///source") as s0, connect("postgresql:///target") as s1:
    m = Migration(s0, s1)
    m.set_safety(False)  # allow drops
    m.add_all_changes()
    print(m.sql)
```

## Supported objects

| Object | Notes |
| --- | --- |
| Tables | Including partitioned, inherited, unlogged |
| Views | Including materialized views |
| Functions / Procedures | All languages except C/INTERNAL |
| Constraints | Primary keys, foreign keys, unique, check |
| Indexes | |
| Sequences | Does not track sequence numbers |
| Schemas | |
| Extensions | |
| Enums | |
| Domains | |
| Range types | |
| Collations | |
| Triggers | |
| Row-level security | |
| Rules | |
| Identity columns | |
| Generated columns | |
| Privileges | Requires `--with-privileges` |
| Roles | Requires `--with-roles` |
| Foreign data wrappers | |
| Foreign servers | |
| User mappings | |
| Foreign tables | |
| Text search dictionaries | |
| Text search configurations | |
| Operators | |
| Operator classes | |
| Operator families | |
| Publications | |
| Extended statistics | |
| Event triggers | |
| Casts | |
| Comments | |

## CLI reference

| Flag | Description |
| --- | --- |
| `--unsafe` | Allow `DROP` statements |
| `--schema SCHEMA` | Restrict diff to specific schema(s) |
| `--exclude-schema SCHEMA` | Exclude a schema from the diff |
| `--create-extensions-only` | Only output `CREATE EXTENSION` statements |
| `--ignore-extension-versions` | Ignore version differences for extensions |
| `--with-privileges` | Include `GRANT`/`REVOKE` statements |
| `--with-roles` | Include role diffing |
| `--force-utf8` | Force UTF-8 encoding for output |
| `--concurrent-indexes` | Use `CREATE INDEX CONCURRENTLY` |

### Connection URLs

Standard PostgreSQL connection URLs:

```
postgresql://username:password@hostname/databasename
postgresql:///mydatabase  # local with trust auth
```

See the [PostgreSQL docs](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING)
for full URL format details.

## Contributing

```bash
just install     # uv sync
just test        # run all tests (requires local PostgreSQL)
just lint        # ruff check
just fmt         # ruff format + fix
just typecheck   # ty check
```

Tests run against a real PostgreSQL instance. CI runs across Python 3.10-3.13 and PostgreSQL 14-18.

```bash
just test-pg 16      # test against PG 16 via Docker
just test-pg-all     # test against PG 14-18
```

## Acknowledgements

Originally created by [Robert Lechte](https://github.com/djrobstep). This fork is maintained by
[r614](https://github.com/r614).

## License

[MIT](LICENSE)
