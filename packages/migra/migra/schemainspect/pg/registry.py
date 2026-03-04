from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal


@dataclass
class ObjectType:
    """Descriptor for a PostgreSQL object type."""

    name: str
    schema_filterable: bool = True
    include_in_eq: bool = True
    min_pg_version: int = 14


@dataclass
class DiffStep:
    """A single step in the DDL diff ordering."""

    name: str
    kwargs: dict[str, Any] = field(default_factory=dict)
    condition: Literal["privileges", "roles"] | None = None


REGISTRY: dict[str, ObjectType] = {}

COMPOUND_PROPS = ["relations", "tables", "views", "functions", "selectables"]

DIFF_STEPS: list[DiffStep] = [
    DiffStep("roles", {"creations_only": True}, condition="roles"),
    DiffStep("schemas", {"creations_only": True}),
    DiffStep("extensions", {"creations_only": True, "modifications": False}),
    DiffStep("extensions", {"modifications_only": True, "modifications": True}),
    DiffStep("collations", {"creations_only": True}),
    DiffStep("enums", {"creations_only": True, "modifications": False}),
    DiffStep("domains", {"creations_only": True}),
    DiffStep("range_types", {"creations_only": True}),
    DiffStep("sequences", {"creations_only": True}),
    DiffStep("triggers", {"drops_only": True}),
    DiffStep("rlspolicies", {"drops_only": True, "modifications": False}),
    DiffStep("privileges", {"drops_only": True}, condition="privileges"),
    DiffStep("non_pk_constraints", {"drops_only": True}),
    DiffStep("comments", {"drops_only": True}),
    DiffStep("mv_indexes", {"drops_only": True}),
    DiffStep("non_table_selectable_drops"),
    DiffStep("pk_constraints", {"drops_only": True}),
    DiffStep("non_mv_indexes", {"drops_only": True}),
    DiffStep("tables_only_selectables"),
    DiffStep("sequences", {"drops_only": True}),
    DiffStep("range_types", {"drops_only": True}),
    DiffStep("domains", {"drops_only": True}),
    DiffStep("enums", {"drops_only": True, "modifications": False}),
    DiffStep("extensions", {"drops_only": True, "modifications": False}),
    DiffStep("non_mv_indexes", {"creations_only": True}),
    DiffStep("pk_constraints", {"creations_only": True}),
    DiffStep("non_pk_constraints", {"creations_only": True}),
    DiffStep("non_table_selectable_creations"),
    DiffStep("mv_indexes", {"creations_only": True}),
    DiffStep("privileges", {"creations_only": True}, condition="privileges"),
    DiffStep("rlspolicies", {"modifications_only": True}),
    DiffStep("rlspolicies", {"creations_only": True, "modifications": False}),
    DiffStep("triggers", {"creations_only": True}),
    DiffStep("comments", {"creations_only": True}),
    DiffStep("collations", {"drops_only": True}),
    DiffStep("roles", {"drops_only": True}, condition="roles"),
    DiffStep("schemas", {"drops_only": True}),
]


def register(obj_type: ObjectType) -> ObjectType:
    REGISTRY[obj_type.name] = obj_type
    return obj_type
