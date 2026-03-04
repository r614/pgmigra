from ...inspected import Inspected
from ...misc import quoted_identifier
from ..registry import ObjectType, register


class InspectedSequence(Inspected):
    def __init__(self, name, schema, table_name=None, column_name=None):
        self.name = name
        self.schema = schema
        self.table_name = table_name
        self.column_name = column_name

    @property
    def drop_statement(self):
        return f"drop sequence if exists {self.quoted_full_name};"

    @property
    def create_statement(self):
        return f"create sequence {self.quoted_full_name};"

    @property
    def create_statement_with_ownership(self):
        t_col_name = self.quoted_table_and_column_name

        if self.table_name and self.column_name:
            return f"create sequence {self.quoted_full_name} owned by {t_col_name};"
        else:
            return f"create sequence {self.quoted_full_name};"

    @property
    def alter_ownership_statement(self):
        t_col_name = self.quoted_table_and_column_name

        if t_col_name is not None:
            return f"alter sequence {self.quoted_full_name} owned by {t_col_name};"
        else:
            return f"alter sequence {self.quoted_full_name} owned by none;"

    @property
    def quoted_full_table_name(self):
        if self.table_name is not None:
            return quoted_identifier(self.table_name, self.schema)

    @property
    def quoted_table_and_column_name(self):
        if self.column_name is not None and self.table_name is not None:
            return (
                self.quoted_full_table_name + "." + quoted_identifier(self.column_name)
            )

    def __eq__(self, other):
        equalities = (
            self.name == other.name,
            self.schema == other.schema,
            self.quoted_table_and_column_name == other.quoted_table_and_column_name,
        )
        return all(equalities)


register(ObjectType(name="sequences"))
