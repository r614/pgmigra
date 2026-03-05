from ...inspected import Inspected, TableRelated

COMMANDTYPES = {"*": "all", "r": "select", "a": "insert", "w": "update", "d": "delete"}


class InspectedRowPolicy(Inspected, TableRelated):
    def __init__(
        self, name, schema, table_name, commandtype, permissive, roles, qual, withcheck
    ):
        self.name = name
        self.schema = schema
        self.table_name = table_name
        self.commandtype = commandtype
        self.permissive = permissive
        self.roles = roles
        self.qual = qual
        self.withcheck = withcheck

    @property
    def permissiveness(self):
        return "permissive" if self.permissive else "restrictive"

    @property
    def commandtype_keyword(self):
        return COMMANDTYPES[self.commandtype]

    @property
    def key(self):
        return f"{self.quoted_full_table_name}.{self.quoted_name}"

    @property
    def create_statement(self):
        if self.qual:
            qual_clause = f"\nusing ({self.qual})"
        else:
            qual_clause = ""

        if self.withcheck:
            withcheck_clause = f"\nwith check ({self.withcheck})"
        else:
            withcheck_clause = ""

        roleslist = ", ".join(self.roles)

        return f"""create policy {self.quoted_name}
on {self.quoted_full_table_name}
as {self.permissiveness}
for {self.commandtype_keyword}
to {roleslist}{qual_clause}{withcheck_clause};
"""

    @property
    def drop_statement(self):
        return f"drop policy {self.quoted_name} on {self.quoted_full_table_name};"

    @property
    def alter_statement(self):
        parts = [f"alter policy {self.quoted_name} on {self.quoted_full_table_name}"]

        roleslist = ", ".join(self.roles)
        parts.append(f"to {roleslist}")

        if self.qual:
            parts.append(f"using ({self.qual})")

        if self.withcheck:
            parts.append(f"with check ({self.withcheck})")

        return "\n".join(parts) + ";"

    def alter_statements(self, other):
        if self.permissive != other.permissive or self.commandtype != other.commandtype:
            return [other.drop_statement, self.create_statement]
        return [self.alter_statement]

    def __eq__(self, other):
        equalities = (
            self.name == other.name,
            self.schema == other.schema,
            self.table_name == other.table_name,
            self.permissive == other.permissive,
            self.commandtype == other.commandtype,
            self.roles == other.roles,
            self.qual == other.qual,
            self.withcheck == other.withcheck,
        )
        return all(equalities)
