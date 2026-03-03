# Contributing

## Before coding

Open an issue first to discuss the approach. This helps avoid wasted effort and gives maintainers a chance to provide guidance.

## Structure

The source lives in `packages/migra/`. Schema introspection code is in `migra/schemainspect/` as a subpackage, and diffing/SQL generation code is in the top-level `migra/` package.

## Development

```bash
just install     # install dependencies
just test        # run all tests (requires local PostgreSQL)
just lint        # run linter
just fmt         # format code
just typecheck   # run type checker (ty)
```

To test against a specific PostgreSQL version via Docker:

```bash
just test-pg 16          # run tests against PG 16
just test-pg-all         # run tests against PG 14, 15, 16, 17
just test-pg-stop        # stop all test containers
```

## Pull requests

- Keep PRs small and focused — ideally under 200 lines
- Add tests for new functionality
- Run `just check && just typecheck` before submitting

### Adding a test fixture

If your change affects schema diffing, add a test fixture in `packages/migra/tests/FIXTURES/`:

1. Create a directory named after your fixture (e.g. `packages/migra/tests/FIXTURES/myfeature/`)
2. Add these files:
   - `a.sql` — the "before" schema
   - `b.sql` — the "after" schema
   - `additions.sql` — extra SQL applied after initial diff (can be empty)
   - `expected.sql` — expected migration output from `a` to `b`
   - `expected2.sql` — expected migration output after applying `additions.sql` (can be empty)
3. Add the fixture name to the `FIXTURE_NAMES` list in `packages/migra/tests/test_migra.py`
