from ...inspected import Inspected
from ...misc import quoted_identifier
from ..registry import ObjectType, register


class InspectedEnum(Inspected):
    def __init__(self, name, schema, elements):
        self.name = name
        self.schema = schema
        self.elements = elements
        self.dependents = []
        self.dependent_on = []

    @property
    def drop_statement(self):
        return f"drop type {self.quoted_full_name};"

    @property
    def create_statement(self):
        return f"create type {self.quoted_full_name} as enum ({self.quoted_elements});"

    @property
    def quoted_elements(self):
        quoted = [f"'{e}'" for e in self.elements]
        return ", ".join(quoted)

    def alter_rename_statement(self, new_name):
        name = new_name

        return (
            f"alter type {self.quoted_full_name} rename to {quoted_identifier(name)};"
        )

    def drop_statement_with_rename(self, new_name):
        name = new_name
        new_name = quoted_identifier(name, self.schema)
        return f"drop type {new_name};"

    def change_statements(self, new):
        if not self.can_be_changed_to(new):
            raise ValueError

        new = new.elements
        old = self.elements
        statements = []
        previous = None
        for c in new:
            if c not in old:
                if not previous:
                    s = f"alter type {self.quoted_full_name} add value '{c}' before '{old[0]}';"
                else:
                    s = f"alter type {self.quoted_full_name} add value '{c}' after '{previous}';"
                statements.append(s)
            previous = c
        return statements

    def can_be_changed_to(self, new):
        old = self.elements
        return [e for e in new.elements if e in old] == old

    def __eq__(self, other):
        equalities = (
            self.name == other.name,
            self.schema == other.schema,
            self.elements == other.elements,
        )
        return all(equalities)


register(ObjectType(name="enums"))
