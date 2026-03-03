from __future__ import annotations

from itertools import zip_longest
from typing import Any


def transposed(in_data: list[Any]) -> list[tuple[Any, ...]]:
    return list(zip_longest(*in_data))


def rows_to_table(rows: list[Any], sep: str = " ") -> str:
    rows_out = [sep.join(values) for r in rows for values in r]
    return "\n" + "\n".join(rows_out) + "\n"


def t(rows: list[Any], sep: str = " ", markdown: bool = True) -> str | None:
    is_dicts = rows and all(hasattr(rows[0], attr) for attr in "keys values".split())
    if not rows:
        return None

    if is_dicts:
        keys = list(rows[0].keys())
        rows = [_.values() for _ in rows]
    else:
        keys = rows[0]

    if markdown:
        sep = " | "

    rows = [keys] + rows

    columns = transposed(rows)
    widths = [max(len(str(_)) for _ in c) for c in columns]

    rows_out = [
        sep.join(str(value).ljust(widths[i]) for i, value in enumerate(r)) for r in rows
    ]

    if markdown:
        rows_out.insert(1, sep.join(["-" * w for w in widths]))

    return "\n".join(rows_out)
