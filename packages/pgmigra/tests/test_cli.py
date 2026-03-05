from pgmigra.command import parse_args


def test_exclude_schema_flag():
    """Verify --exclude-schema flag works (kebab-case)."""
    args = parse_args(["--exclude-schema", "myschema", "--unsafe", "EMPTY", "EMPTY"])
    assert args.exclude_schema == "myschema"
