from ...inspected import Inspected
from ...misc import quoted_identifier


class InspectedType(Inspected):
    def __init__(self, name, schema, columns):
        self.name = name
        self.schema = schema
        self.columns = columns

    @property
    def drop_statement(self):
        return f"drop type {self.signature};"

    @property
    def create_statement(self):
        sql = f"create type {self.signature} as (\n"

        indent = " " * 4
        typespec = [
            f"{indent}{quoted_identifier(name)} {_type}"
            for name, _type in self.columns.items()
        ]

        sql += ",\n".join(typespec)
        sql += "\n);"
        return sql

    def __eq__(self, other):
        return (
            self.schema == other.schema
            and self.name == other.name
            and self.columns == other.columns
        )
