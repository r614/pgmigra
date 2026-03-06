from ...inspected import Inspected
from ...misc import quoted_identifier


class InspectedFDW(Inspected):
    def __init__(
        self,
        name,
        owner,
        handler_name,
        handler_schema,
        validator_name,
        validator_schema,
        options,
    ):
        self.name = name
        self.schema = ""
        self.owner = owner
        self.handler_name = handler_name
        self.handler_schema = handler_schema
        self.validator_name = validator_name
        self.validator_schema = validator_schema
        self.options = options

    @property
    def quoted_full_name(self):
        return quoted_identifier(self.name)

    @property
    def create_statement(self):
        stmt = f"CREATE FOREIGN DATA WRAPPER {self.quoted_full_name}"
        if self.handler_name:
            handler = quoted_identifier(self.handler_name, schema=self.handler_schema)
            stmt += f" HANDLER {handler}"
        if self.validator_name:
            validator = quoted_identifier(
                self.validator_name, schema=self.validator_schema
            )
            stmt += f" VALIDATOR {validator}"
        if self.options:
            stmt += f" OPTIONS ({self.options})"
        stmt += ";"
        if self.owner:
            stmt += f"\nALTER FOREIGN DATA WRAPPER {self.quoted_full_name} OWNER TO {quoted_identifier(self.owner)};"
        return stmt

    @property
    def drop_statement(self):
        return f"DROP FOREIGN DATA WRAPPER {self.quoted_full_name};"

    def __eq__(self, other):
        return (
            self.name == other.name
            and self.owner == other.owner
            and self.handler_name == other.handler_name
            and self.handler_schema == other.handler_schema
            and self.validator_name == other.validator_name
            and self.validator_schema == other.validator_schema
            and self.options == other.options
        )
