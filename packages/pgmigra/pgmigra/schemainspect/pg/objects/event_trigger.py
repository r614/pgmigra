from ...inspected import Inspected
from ...misc import quoted_identifier


class InspectedEventTrigger(Inspected):
    def __init__(
        self, name, owner, event, enabled, tags, function_name, function_schema
    ):
        self.name = name
        self.schema = ""
        self.owner = owner
        self.event = event
        self.enabled = enabled
        self.tags = tags
        self.function_name = function_name
        self.function_schema = function_schema

    @property
    def quoted_full_name(self):
        return quoted_identifier(self.name)

    @property
    def create_statement(self):
        stmt = f"CREATE EVENT TRIGGER {self.quoted_full_name} ON {self.event}"
        if self.tags:
            tag_list = ", ".join(f"'{t.strip()}'" for t in self.tags.split(","))
            stmt += f"\n  WHEN TAG IN ({tag_list})"
        func = quoted_identifier(self.function_name, schema=self.function_schema)
        stmt += f"\n  EXECUTE FUNCTION {func}();"
        if self.enabled and self.enabled != "O":
            status = {
                "D": "DISABLE",
                "R": "ENABLE REPLICA",
                "A": "ENABLE ALWAYS",
            }
            stmt += (
                f"\nALTER EVENT TRIGGER {self.quoted_full_name} {status[self.enabled]};"
            )
        if self.owner:
            stmt += f"\nALTER EVENT TRIGGER {self.quoted_full_name} OWNER TO {quoted_identifier(self.owner)};"
        return stmt

    @property
    def drop_statement(self):
        return f"DROP EVENT TRIGGER {self.quoted_full_name};"

    def __eq__(self, other):
        return (
            self.name == other.name
            and self.owner == other.owner
            and self.event == other.event
            and self.enabled == other.enabled
            and self.tags == other.tags
            and self.function_name == other.function_name
            and self.function_schema == other.function_schema
        )
