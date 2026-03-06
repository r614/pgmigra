from ...inspected import Inspected
from ...misc import quoted_identifier


class InspectedForeignServer(Inspected):
    def __init__(self, name, fdw_name, owner, server_type, server_version, options):
        self.name = name
        self.schema = ""
        self.fdw_name = fdw_name
        self.owner = owner
        self.server_type = server_type
        self.server_version = server_version
        self.options = options

    @property
    def quoted_full_name(self):
        return quoted_identifier(self.name)

    @property
    def create_statement(self):
        stmt = f"CREATE SERVER {self.quoted_full_name}"
        if self.server_type:
            stmt += f" TYPE '{self.server_type}'"
        if self.server_version:
            stmt += f" VERSION '{self.server_version}'"
        stmt += f" FOREIGN DATA WRAPPER {quoted_identifier(self.fdw_name)}"
        if self.options:
            stmt += f" OPTIONS ({self.options})"
        stmt += ";"
        if self.owner:
            stmt += f"\nALTER SERVER {self.quoted_full_name} OWNER TO {quoted_identifier(self.owner)};"
        return stmt

    @property
    def drop_statement(self):
        return f"DROP SERVER {self.quoted_full_name};"

    def __eq__(self, other):
        return (
            self.name == other.name
            and self.fdw_name == other.fdw_name
            and self.owner == other.owner
            and self.server_type == other.server_type
            and self.server_version == other.server_version
            and self.options == other.options
        )
