from ...inspected import Inspected
from ...misc import quoted_identifier
from ..registry import ObjectType, register


class InspectedComment(Inspected):
    def __init__(self, object_type, schema, name, column_name, comment):
        self.object_type = object_type
        self.schema = schema
        self.name = name
        self.column_name = column_name
        self.comment = comment

    @property
    def quoted_full_name(self):
        if self.column_name:
            return f"{quoted_identifier(self.schema)}.{quoted_identifier(self.name)}.{quoted_identifier(self.column_name)}"
        return quoted_identifier(self.name, schema=self.schema)

    @property
    def key(self):
        if self.column_name:
            return f"{self.object_type}:{self.schema}.{self.name}.{self.column_name}"
        return f"{self.object_type}:{self.schema}.{self.name}"

    @property
    def target_clause(self):
        if self.column_name:
            return f"column {quoted_identifier(self.schema)}.{quoted_identifier(self.name)}.{quoted_identifier(self.column_name)}"
        return f"{self.object_type} {quoted_identifier(self.name, schema=self.schema)}"

    @property
    def create_statement(self):
        escaped = self.comment.replace("'", "''")
        return f"comment on {self.target_clause} is '{escaped}';"

    @property
    def drop_statement(self):
        return f"comment on {self.target_clause} is null;"

    def __eq__(self, other):
        return (
            self.object_type == other.object_type
            and self.schema == other.schema
            and self.name == other.name
            and self.column_name == other.column_name
            and self.comment == other.comment
        )


register(ObjectType(name="comments"))
