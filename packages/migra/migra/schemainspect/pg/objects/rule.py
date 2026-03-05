from ...inspected import Inspected, TableRelated
from ...misc import quoted_identifier


class InspectedRule(Inspected, TableRelated):
    def __init__(self, name, schema, table_name, enabled, definition):
        self.name = name
        self.schema = schema
        self.table_name = table_name
        self.enabled = enabled
        self.definition = definition

    @property
    def quoted_full_name(self):
        return f"{quoted_identifier(self.schema)}.{quoted_identifier(self.table_name)}.{quoted_identifier(self.name)}"

    @property
    def drop_statement(self):
        return f"DROP RULE {quoted_identifier(self.name)} ON {quoted_identifier(self.schema)}.{quoted_identifier(self.table_name)};"

    @property
    def create_statement(self):
        status_sql = {
            "O": "ENABLE RULE",
            "D": "DISABLE RULE",
            "R": "ENABLE REPLICA RULE",
            "A": "ENABLE ALWAYS RULE",
        }
        stmt = self.definition + ";"
        if self.enabled in ("D", "R", "A"):
            schema = quoted_identifier(self.schema)
            table = quoted_identifier(self.table_name)
            rule_name = quoted_identifier(self.name)
            stmt += f"\nALTER TABLE {schema}.{table} {status_sql[self.enabled]} {rule_name};"
        return stmt

    def __eq__(self, other):
        return (
            self.name == other.name
            and self.schema == other.schema
            and self.table_name == other.table_name
            and self.enabled == other.enabled
            and self.definition == other.definition
        )
