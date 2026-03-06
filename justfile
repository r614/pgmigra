default:
    @just --list

# Install all dependencies
install:
    uv sync

# Run all tests (parallel via pytest-xdist)
test *pytest_args="":
    uv run pytest packages/pgmigra/tests -n auto -x --tb=short {{pytest_args}}

# Run tests serially (for debugging)
test-seq *pytest_args="":
    uv run pytest packages/pgmigra/tests -x -svv --tb=short {{pytest_args}}

# Run tests with coverage
test-cov:
    uv run pytest packages/pgmigra/tests -x --cov=packages/pgmigra/migra --cov-report=term-missing

# Lint
lint:
    uv run ruff check .

# Format
fmt:
    uv run ruff format .
    uv run ruff check --fix .

# Check formatting without modifying
check:
    uv run ruff check .
    uv run ruff format --check .

# Type check
typecheck:
    uv run ty check

# Run tests against a specific PG version (14-18) via Docker
test-pg version="16" *pytest_args="":
    #!/usr/bin/env bash
    set -euo pipefail
    CONTAINER="pgmigra-test-pg{{version}}"
    PORT=$((5430 + {{version}}))
    if ! docker ps --format '{{"{{"}}.Names{{"}}"}}' | grep -q "^${CONTAINER}$"; then
        docker rm -f "$CONTAINER" 2>/dev/null || true
        docker run -d --name "$CONTAINER" \
            -e POSTGRES_HOST_AUTH_METHOD=trust \
            -p "${PORT}:5432" \
            postgres:{{version}}
        until docker exec "$CONTAINER" pg_isready -U postgres > /dev/null 2>&1; do sleep 0.2; done
        docker exec "$CONTAINER" psql -U postgres -c "CREATE ROLE schemainspect_test_role" 2>/dev/null || true
    fi
    PGHOST=localhost PGPORT=$PORT PGUSER=postgres \
        uv run pytest packages/pgmigra/tests -x --tb=short {{pytest_args}}

# Run tests against all supported PG versions (sequentially)
test-pg-all-seq *pytest_args="":
    just test-pg 14 {{pytest_args}}
    just test-pg 15 {{pytest_args}}
    just test-pg 16 {{pytest_args}}
    just test-pg 17 {{pytest_args}}
    just test-pg 18 {{pytest_args}}

# Run tests against all PG versions in parallel (default)
test-pg-all *pytest_args="":
    #!/usr/bin/env bash
    set -euo pipefail
    pids=()
    versions=(14 15 16 17 18)
    for v in "${versions[@]}"; do
        just test-pg "$v" {{pytest_args}} > /tmp/pgmigra-pg${v}.log 2>&1 &
        pids+=($!)
    done
    failed=0
    for i in "${!versions[@]}"; do
        v=${versions[$i]}
        if wait "${pids[$i]}"; then
            echo "PG ${v}: $(tail -1 /tmp/pgmigra-pg${v}.log)"
        else
            echo "PG ${v}: FAILED"
            tail -5 /tmp/pgmigra-pg${v}.log
            failed=1
        fi
    done
    exit $failed

# Start all test PG containers (14-18)
test-pg-start:
    #!/usr/bin/env bash
    for v in 14 15 16 17 18; do
        CONTAINER="pgmigra-test-pg${v}"
        PORT=$((5430 + v))
        if ! docker ps --format '{{"{{"}}.Names{{"}}"}}' | grep -q "^${CONTAINER}$"; then
            docker rm -f "$CONTAINER" 2>/dev/null || true
            docker run -d --name "$CONTAINER" \
                -e POSTGRES_HOST_AUTH_METHOD=trust \
                -p "${PORT}:5432" \
                "postgres:${v}"
        fi
    done
    for v in 14 15 16 17 18; do
        CONTAINER="pgmigra-test-pg${v}"
        until docker exec "$CONTAINER" pg_isready -U postgres > /dev/null 2>&1; do sleep 0.2; done
    done
    echo "All PG containers ready"

# Stop all test PG containers
test-pg-stop:
    docker rm -f pgmigra-test-pg14 pgmigra-test-pg15 pgmigra-test-pg16 pgmigra-test-pg17 pgmigra-test-pg18 2>/dev/null || true
