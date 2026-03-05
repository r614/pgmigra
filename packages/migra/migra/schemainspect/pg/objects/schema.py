from ...inspected import Inspected
from ...misc import quoted_identifier


class InspectedSchema(Inspected):
    def __init__(self, schema):
        self.schema = schema
        self.name = ""

    @property
    def create_statement(self):
        return f"create schema if not exists {self.quoted_schema};"

    @property
    def drop_statement(self):
        return f"drop schema if exists {self.quoted_schema};"

    @property
    def quoted_full_name(self):
        return self.quoted_name

    @property
    def quoted_name(self):
        return quoted_identifier(self.schema)

    def __eq__(self, other):
        return self.schema == other.schema
