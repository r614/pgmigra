from ...inspected import Inspected
from ..registry import ObjectType, register


class InspectedRangeType(Inspected):
    def __init__(
        self,
        name,
        schema,
        subtype,
        collation,
        subtype_opclass,
        canonical,
        subtype_diff,
    ):
        self.name = name
        self.schema = schema
        self.subtype = subtype
        self.collation = collation
        self.subtype_opclass = subtype_opclass
        self.canonical = canonical
        self.subtype_diff = subtype_diff

    @property
    def create_statement(self):
        parts = [f"subtype = {self.subtype}"]
        if self.subtype_opclass:
            parts.append(f"subtype_opclass = {self.subtype_opclass}")
        if self.collation:
            parts.append(f"collation = {self.collation}")
        if self.canonical:
            parts.append(f"canonical = {self.canonical}")
        if self.subtype_diff:
            parts.append(f"subtype_diff = {self.subtype_diff}")
        options = ",\n    ".join(parts)
        return f"create type {self.quoted_full_name} as range (\n    {options}\n);"

    @property
    def drop_statement(self):
        return f"drop type {self.quoted_full_name};"

    equality_attributes = (
        "schema name subtype collation subtype_opclass canonical subtype_diff".split()
    )

    def __eq__(self, other):
        try:
            return all(
                getattr(self, a) == getattr(other, a) for a in self.equality_attributes
            )
        except AttributeError:
            return False


register(ObjectType(name="range_types"))
