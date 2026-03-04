from __future__ import annotations

DQ = '"'


def quoted_identifier(
    identifier: str | None,
    schema: str | None = None,
    identity_arguments: str | None = None,
) -> str:
    if identifier is None and schema is not None:
        return f"{DQ}{schema.replace(DQ, DQ * 2)}{DQ}"
    s = f"{DQ}{str(identifier).replace(DQ, DQ * 2)}{DQ}"
    if schema:
        s = f"{DQ}{schema.replace(DQ, DQ * 2)}{DQ}.{s}"
    if identity_arguments is not None:
        s = f"{s}({identity_arguments})"
    return s
