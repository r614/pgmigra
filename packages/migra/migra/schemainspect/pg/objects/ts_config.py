from ...inspected import Inspected
from ...misc import quoted_identifier


class InspectedTSConfig(Inspected):
    def __init__(self, name, schema, parser_name, parser_schema, mappings=None):
        self.name = name
        self.schema = schema
        self.parser_name = parser_name
        self.parser_schema = parser_schema
        self.mappings = mappings or {}

    @property
    def create_statement(self):
        parser = quoted_identifier(self.parser_name, schema=self.parser_schema)
        stmt = f"CREATE TEXT SEARCH CONFIGURATION {self.quoted_full_name} (\n  PARSER = {parser}\n);"
        for token_type in sorted(self.mappings):
            dicts = ", ".join(self.mappings[token_type])
            stmt += f"\nALTER TEXT SEARCH CONFIGURATION {self.quoted_full_name}\n  ADD MAPPING FOR {token_type} WITH {dicts};"
        return stmt

    @property
    def drop_statement(self):
        return f"DROP TEXT SEARCH CONFIGURATION {self.quoted_full_name};"

    def __eq__(self, other):
        return (
            self.name == other.name
            and self.schema == other.schema
            and self.parser_name == other.parser_name
            and self.parser_schema == other.parser_schema
            and self.mappings == other.mappings
        )
