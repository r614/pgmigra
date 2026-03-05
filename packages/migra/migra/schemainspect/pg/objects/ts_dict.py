from ...inspected import Inspected
from ...misc import quoted_identifier


class InspectedTSDict(Inspected):
    def __init__(self, name, schema, template_name, template_schema, options):
        self.name = name
        self.schema = schema
        self.template_name = template_name
        self.template_schema = template_schema
        self.options = options

    @property
    def create_statement(self):
        tmpl = quoted_identifier(self.template_name, schema=self.template_schema)
        stmt = f"CREATE TEXT SEARCH DICTIONARY {self.quoted_full_name} (\n  TEMPLATE = {tmpl}"
        if self.options:
            stmt += f",\n  {self.options}"
        stmt += "\n);"
        return stmt

    @property
    def drop_statement(self):
        return f"DROP TEXT SEARCH DICTIONARY {self.quoted_full_name};"

    def __eq__(self, other):
        return (
            self.name == other.name
            and self.schema == other.schema
            and self.template_name == other.template_name
            and self.template_schema == other.template_schema
            and self.options == other.options
        )
