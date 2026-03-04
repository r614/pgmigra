from ...inspected import Inspected
from ...misc import quoted_identifier
from ..registry import ObjectType, register


class InspectedTrigger(Inspected):
    def __init__(
        self, name, schema, table_name, proc_schema, proc_name, enabled, full_definition
    ):
        (
            self.name,
            self.schema,
            self.table_name,
            self.proc_schema,
            self.proc_name,
            self.enabled,
            self.full_definition,
        ) = (name, schema, table_name, proc_schema, proc_name, enabled, full_definition)

        self.dependent_on = [self.quoted_full_selectable_name]
        self.dependents = []

    @property
    def signature(self):
        return self.quoted_full_name

    @property
    def quoted_full_name(self):
        return f"{quoted_identifier(self.schema)}.{quoted_identifier(self.table_name)}.{quoted_identifier(self.name)}"

    @property
    def quoted_full_selectable_name(self):
        return f"{quoted_identifier(self.schema)}.{quoted_identifier(self.table_name)}"

    @property
    def drop_statement(self):
        return f'drop trigger if exists "{self.name}" on "{self.schema}"."{self.table_name}";'

    @property
    def create_statement(self):
        status_sql = {
            "O": "ENABLE TRIGGER",
            "D": "DISABLE TRIGGER",
            "R": "ENABLE REPLICA TRIGGER",
            "A": "ENABLE ALWAYS TRIGGER",
        }
        schema = quoted_identifier(self.schema)
        table = quoted_identifier(self.table_name)
        trigger_name = quoted_identifier(self.name)
        if self.enabled in ("D", "R", "A"):
            table_alter = f"ALTER TABLE {schema}.{table} {status_sql[self.enabled]} {trigger_name}"
            return self.full_definition + ";\n" + table_alter + ";"
        else:
            return self.full_definition + ";"

    def __eq__(self, other):
        return (
            self.name == other.name
            and self.schema == other.schema
            and self.table_name == other.table_name
            and self.proc_schema == other.proc_schema
            and self.proc_name == other.proc_name
            and self.enabled == other.enabled
            and self.full_definition == other.full_definition
        )


register(ObjectType(name="triggers"))
