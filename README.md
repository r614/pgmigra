# migra

[![Test](https://github.com/r614/migra/actions/workflows/test.yml/badge.svg)](https://github.com/r614/migra/actions/workflows/test.yml)

Like `diff` but for PostgreSQL schemas. Compares two database schemas and generates the SQL statements needed to make them match.

```bash
$ migra postgresql:///a postgresql:///b
alter table "public"."products" add column newcolumn text;

alter table "public"."products" add constraint "x" CHECK ((price > (0)::numeric));
```

This is an opinionated fork/rewrite of [djrobstep/migra](https://github.com/djrobstep/migra), which is no longer maintained. It fixes some core issues and modernizes the repo, bundling [schemainspect](https://github.com/djrobstep/schemainspect) into a single monorepo.

## Installation

Install directly from GitHub:

```bash
pip install "migra @ git+https://github.com/r614/migra.git#subdirectory=packages/migra"
```

Or with `uv`:

```bash
uv add "migra @ git+https://github.com/r614/migra.git#subdirectory=packages/migra"
```

## Features

| Feature | Notes |
|---------|-------|
| Tables | Including partitioned, inherited, unlogged |
| Views | Including materialized views |
| Functions / Procedures | All languages except C/INTERNAL |
| Constraints | Primary keys, foreign keys, unique, check |
| Indexes | |
| Sequences | Does not track sequence numbers |
| Schemas | |
| Extensions | |
| Enums | |
| Privileges | Requires `--with-privileges` flag |
| Row-level security | |
| Triggers | |
| Identity columns | |
| Generated columns | |
| Collations | |
| Custom types/domains | Drop-and-create only, no alter |

Extension-managed schema contents are ignored and left to the extension to manage. View and function dependencies are handled automatically — migra drops and creates them in the correct order.

## Usage

### Comparing two databases

```bash
migra postgresql:///source postgresql:///target
```

This outputs the SQL required to transform `source`'s schema to match `target`. Empty output means the schemas already match.

### Safety

By default, migra exits with an error if any `DROP` statements would be generated. Use `--unsafe` to allow destructive statements:

```bash
migra --unsafe postgresql:///source postgresql:///target
```

### Generating a migration script

```bash
migra --unsafe postgresql:///production postgresql:///target > migration.sql
```

Review the script, then apply it:

```bash
psql postgresql:///production -1 -f migration.sql
```

The `-1` flag wraps the migration in a single transaction.

### Comparing against an empty database

Use `EMPTY` as the source to generate full schema creation SQL:

```bash
migra --unsafe EMPTY postgresql:///mydb
```

### CLI options

| Flag | Description |
|------|-------------|
| `--unsafe` | Allow `DROP` statements (default: error on drops) |
| `--schema SCHEMA` | Restrict diff to a single schema |
| `--exclude_schema SCHEMA` | Exclude a schema from the diff |
| `--create-extensions-only` | Only output `CREATE EXTENSION` statements |
| `--ignore-extension-versions` | Ignore version differences for extensions |
| `--with-privileges` | Include `GRANT`/`REVOKE` statements |
| `--force-utf8` | Force UTF-8 encoding for output |

### Connection URLs

migra uses standard PostgreSQL connection URLs:

```
postgresql://username:password@hostname/databasename
```

For local connections with trust authentication:

```
postgresql:///mydatabase
```

See the [PostgreSQL docs](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING) for full URL format details.

## Python API

### Basic usage

```python
from migra import Migration
from migra.db import connect

with connect("postgresql:///source") as s0, connect("postgresql:///target") as s1:
    m = Migration(s0, s1)
    m.set_safety(False)  # allow drops
    m.add_all_changes()
    print(m.sql)
```

### Auto-syncing a dev database

```python
from migra import Migration
from migra.db import connect, temporary_database

DB_URL = "postgresql:///my_dev_db"

with temporary_database() as temp_url:
    load_target_schema(temp_url)  # your setup function

    with connect(DB_URL) as s_current, connect(temp_url) as s_target:
        m = Migration(s_current, s_target)
        m.set_safety(False)
        m.add_all_changes()

        if m.statements:
            print("Pending changes:\n")
            print(m.sql)
            if input("Apply? ") == "yes":
                m.apply()
        else:
            print("Already synced.")
```

### Applying changes programmatically

```python
from migra import Migration
from migra.db import connect

with connect("postgresql:///source") as s0, connect("postgresql:///target") as s1:
    m = Migration(s0, s1)
    m.set_safety(False)
    m.add_all_changes()

    if m.statements:
        m.apply()  # executes statements against source db
        # source schema now matches target
```

### Using pre-inspected schemas

If you already have inspector objects, pass them directly to avoid redundant introspection:

```python
from migra import Migration, get_inspector
from migra.db import connect

with connect("postgresql:///source") as s0, connect("postgresql:///target") as s1:
    i0 = get_inspector(s0)
    i1 = get_inspector(s1)
    m = Migration(i0, i1)
    m.set_safety(False)
    m.add_all_changes()
    print(m.sql)
```

### Including privileges

```python
m.add_all_changes(privileges=True)
```

### Extension-only changes

```python
m.add_extension_changes(drops=False)  # only CREATE EXTENSION statements
```

## Development

The source lives in `packages/migra/`, which includes `schemainspect` as a subpackage for PostgreSQL schema introspection.

### Setup

```bash
just install     # uv sync
just test        # run all tests (requires local PostgreSQL)
just lint        # ruff check
just fmt         # ruff format + fix
just typecheck   # ty check
```

### Testing

Tests run against a real PostgreSQL instance. CI runs across a matrix of Python 3.10-3.13 and PostgreSQL 14-17.

```bash
just test                # all tests
just test-cov            # with coverage
```

To test against a specific PostgreSQL version locally via Docker:

```bash
just test-pg 16          # run tests against PG 16
just test-pg-all         # run tests against PG 14, 15, 16, 17
just test-pg-stop        # stop all test containers
```

## Credits

Originally created by [Robert Lechte](https://github.com/djrobstep). This fork is maintained by [r614](https://github.com/r614).

## License

[MIT](LICENSE)
