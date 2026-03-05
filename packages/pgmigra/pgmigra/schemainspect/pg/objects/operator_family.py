from ...inspected import Inspected
from ...misc import quoted_identifier


class InspectedOperatorFamily(Inspected):
    def __init__(self, name, schema, access_method):
        self.name = name
        self.schema = schema
        self.access_method = access_method

    @property
    def key(self):
        return f"{quoted_identifier(self.name, schema=self.schema)} USING {self.access_method}"

    @property
    def quoted_full_name(self):
        return self.key

    @property
    def create_statement(self):
        qn = quoted_identifier(self.name, schema=self.schema)
        return f"CREATE OPERATOR FAMILY {qn} USING {self.access_method};"

    @property
    def drop_statement(self):
        qn = quoted_identifier(self.name, schema=self.schema)
        return f"DROP OPERATOR FAMILY {qn} USING {self.access_method};"

    def __eq__(self, other):
        return (
            self.name == other.name
            and self.schema == other.schema
            and self.access_method == other.access_method
        )
