default:
    @just --list

# Install all dependencies
install:
    uv sync

# Run all tests
test:
    uv run pytest packages/migra/tests -x -svv --tb=short

# Run tests with coverage
test-cov:
    uv run pytest packages/migra/tests -x --cov=packages/migra/migra --cov-report=term-missing

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

# Run tests against a specific PG version (14-17) via Docker
test-pg version="16":
    #!/usr/bin/env bash
    set -euo pipefail
    CONTAINER="migra-test-pg{{version}}"
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
        uv run pytest packages/migra/tests -x -svv --tb=short

# Run tests against all supported PG versions
test-pg-all:
    just test-pg 14
    just test-pg 15
    just test-pg 16
    just test-pg 17

# Stop all test PG containers
test-pg-stop:
    docker rm -f migra-test-pg14 migra-test-pg15 migra-test-pg16 migra-test-pg17 2>/dev/null || true
