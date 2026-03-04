from ...inspected import Inspected
from ..registry import ObjectType, register


class InspectedDomain(Inspected):
    def __init__(
        self,
        name,
        schema,
        data_type,
        collation,
        constraint_name,
        not_null,
        default,
        check,
    ):
        self.name = name
        self.schema = schema
        self.data_type = data_type
        self.collation = collation
        self.constraint_name = constraint_name
        self.not_null = not_null
        self.default = default
        self.check = check

    @property
    def drop_statement(self):
        return f"drop domain {self.signature};"

    @property
    def create_statement(self):
        return f"""\
create domain {self.signature}
as {self.data_type}
{self.collation_clause}{self.default_clause}{self.nullable_clause}{self.check_clause}
"""

    @property
    def check_clause(self):
        if self.check:
            return f"{self.check}\n"

        return ""

    @property
    def collation_clause(self):
        if self.collation:
            return f"collation {self.collation}\n"

        return ""

    @property
    def default_clause(self):
        if self.default:
            return f"default {self.default}\n"

        return ""

    @property
    def nullable_clause(self):
        if self.not_null:
            return "not null\n"
        else:
            return "null\n"

    equality_attributes = (
        "schema name data_type collation default constraint_name not_null check".split()
    )

    def __eq__(self, other):
        try:
            return all(
                [
                    getattr(self, a) == getattr(other, a)
                    for a in self.equality_attributes
                ]
            )
        except AttributeError:
            return False


register(ObjectType(name="domains"))
