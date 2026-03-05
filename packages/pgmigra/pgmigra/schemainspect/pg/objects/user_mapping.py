from ...inspected import Inspected
from ...misc import quoted_identifier


class InspectedUserMapping(Inspected):
    def __init__(self, server_name, user_name, options):
        self.name = user_name or "PUBLIC"
        self.schema = ""
        self.server_name = server_name
        self.user_name = user_name or "PUBLIC"
        self.options = options

    @property
    def quoted_full_name(self):
        return (
            f"{quoted_identifier(self.server_name)}.{quoted_identifier(self.user_name)}"
        )

    @property
    def key(self):
        return self.quoted_full_name

    @property
    def _user_clause(self):
        if self.user_name == "PUBLIC":
            return "PUBLIC"
        return quoted_identifier(self.user_name)

    @property
    def create_statement(self):
        stmt = f"CREATE USER MAPPING FOR {self._user_clause} SERVER {quoted_identifier(self.server_name)}"
        if self.options:
            opts = ", ".join(f"{k} '{v}'" for k, v in _parse_options(self.options))
            stmt += f" OPTIONS ({opts})"
        return stmt + ";"

    @property
    def drop_statement(self):
        return f"DROP USER MAPPING FOR {self._user_clause} SERVER {quoted_identifier(self.server_name)};"

    def __eq__(self, other):
        equalities = [
            self.server_name == other.server_name,
            self.user_name == other.user_name,
        ]
        if self.options is not None and other.options is not None:
            equalities.append(self.options == other.options)
        return all(equalities)


def _parse_options(options):
    if isinstance(options, list):
        for opt in options:
            if "=" in opt:
                k, v = opt.split("=", 1)
                yield k.strip(), v.strip()
    elif isinstance(options, str):
        for opt in options.split(","):
            opt = opt.strip()
            if "=" in opt:
                k, v = opt.split("=", 1)
                yield k.strip(), v.strip()
