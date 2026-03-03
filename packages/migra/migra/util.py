from __future__ import annotations

from typing import TypeVar

V = TypeVar("V")


def differences(
    a: dict[str, V],
    b: dict[str, V],
) -> tuple[dict[str, V], dict[str, V], dict[str, V], dict[str, V]]:
    a_keys = set(a.keys())
    b_keys = set(b.keys())
    keys_added = set(b_keys) - set(a_keys)
    keys_removed = set(a_keys) - set(b_keys)
    keys_common = set(a_keys) & set(b_keys)
    added = {k: b[k] for k in sorted(keys_added)}
    removed = {k: a[k] for k in sorted(keys_removed)}
    modified = {k: b[k] for k in sorted(keys_common) if a[k] != b[k]}
    unmodified = {k: b[k] for k in sorted(keys_common) if a[k] == b[k]}
    return added, removed, modified, unmodified
