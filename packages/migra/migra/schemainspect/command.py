from __future__ import annotations

import argparse
import sys
from io import StringIO
from typing import Any

import psycopg
import yaml
from psycopg.rows import namedtuple_row

from .get import get_inspector
from .misc import quoted_identifier
from .tableformat import t


def parse_args(args: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Inspect a schema")

    subparsers = parser.add_subparsers(help="sub-command help", dest="command")

    parser_deps = subparsers.add_parser("deps", help="Show inspected dependencies")
    parser_deps.add_argument("db_url", help="URL")

    parser_deps2 = subparsers.add_parser(
        "yaml", help="Export schema definition as YAML"
    )
    parser_deps2.add_argument("db_url", help="URL")

    return parser.parse_args(args)


def do_deps(db_url: str) -> None:
    with psycopg.connect(db_url, row_factory=namedtuple_row, autocommit=True) as s:
        i = get_inspector(s)
        deps = i.deps

    def process_row(dep: Any) -> dict[str, str]:
        depends_on = quoted_identifier(dep.name, dep.schema, dep.identity_arguments)
        thing = quoted_identifier(
            dep.name_dependent_on,
            dep.schema_dependent_on,
            dep.identity_arguments_dependent_on,
        )

        return dict(
            thing=f"{dep.kind_dependent_on}: {thing}",
            depends_on=f"{dep.kind}: {depends_on}",
        )

    processed = [process_row(_) for _ in deps]

    rows = t(processed)

    if rows:
        print(rows)
    else:
        print("No dependencies found.")


def do_yaml(db_url: str) -> None:
    with psycopg.connect(db_url, row_factory=namedtuple_row, autocommit=True) as s:
        i = get_inspector(s)
        defn = i.encodeable_definition()

    x = StringIO()

    yaml.safe_dump(defn, x)

    print(x.getvalue())


def run(args: argparse.Namespace) -> None:
    if args.command == "deps":
        do_deps(args.db_url)

    elif args.command == "yaml":
        do_yaml(args.db_url)

    else:
        raise ValueError("no such command")


def do_command() -> None:  # pragma: no cover
    args = parse_args(sys.argv[1:])
    run(args)
