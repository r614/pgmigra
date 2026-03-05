from ...inspected import Inspected


class InspectedExtension(Inspected):
    def __init__(self, name, schema, version=None):
        self.name = name
        self.schema = schema
        self.version = version

    @property
    def drop_statement(self):
        return f"drop extension if exists {self.quoted_name};"

    @property
    def create_statement(self):
        if self.version:
            version_clause = f" version '{self.version}'"
        else:
            version_clause = ""

        return f"create extension if not exists {self.quoted_name} with schema {self.quoted_schema}{version_clause};"

    @property
    def update_statement(self):
        if not self.version:
            return None
        return f"alter extension {self.quoted_name} update to '{self.version}';"

    def alter_statements(self, other=None):
        return [self.update_statement]

    def __eq__(self, other):
        equalities = (
            self.name == other.name,
            self.schema == other.schema,
            self.version == other.version,
        )
        return all(equalities)

    def unversioned_copy(self):
        return InspectedExtension(self.name, self.schema)
