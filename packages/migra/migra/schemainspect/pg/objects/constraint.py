from ...inspected import Inspected, TableRelated
from ...misc import quoted_identifier


class InspectedConstraint(Inspected, TableRelated):
    def __init__(
        self,
        name,
        schema,
        constraint_type,
        table_name,
        definition,
        index,
        is_fk=False,
        is_deferrable=False,
        initially_deferred=False,
    ):
        self.name = name
        self.schema = schema
        self.constraint_type = constraint_type
        self.table_name = table_name
        self.definition = definition
        self.index = index
        self.is_fk = is_fk

        self.quoted_full_foreign_table_name = None
        self.fk_columns_local = None
        self.fk_columns_foreign = None

        self.is_deferrable = is_deferrable
        self.initially_deferred = initially_deferred

    @property
    def drop_statement(self):
        return f"alter table {self.quoted_full_table_name} drop constraint {self.quoted_name};"

    @property
    def deferrable_subclause(self):
        if not self.is_deferrable:
            return ""

        else:
            clause = " DEFERRABLE"

            if self.initially_deferred:
                clause += " INITIALLY DEFERRED"

            return clause

    @property
    def create_statement(self):
        return self.get_create_statement(set_not_valid=False)

    def get_create_statement(self, set_not_valid=False):
        if self.index and self.constraint_type != "EXCLUDE":
            using_clause = f"{self.constraint_type} using index {self.quoted_name}{self.deferrable_subclause}"
        else:
            using_clause = self.definition

            if set_not_valid:
                using_clause += " not valid"

        return f"alter table {self.quoted_full_table_name} add constraint {self.quoted_name} {using_clause};"

    @property
    def can_use_not_valid(self):
        return self.constraint_type in ("CHECK", "FOREIGN KEY") and not self.index

    @property
    def validate_statement(self):
        if self.can_use_not_valid:
            return f"alter table {self.quoted_full_table_name} validate constraint {self.quoted_name};"

    @property
    def safer_create_statements(self):
        if not self.can_use_not_valid:
            return [self.create_statement]

        return [self.get_create_statement(set_not_valid=True), self.validate_statement]

    @property
    def quoted_full_name(self):
        return f"{quoted_identifier(self.schema)}.{quoted_identifier(self.table_name)}.{quoted_identifier(self.name)}"

    def __eq__(self, other):
        equalities = (
            self.name == other.name,
            self.schema == other.schema,
            self.table_name == other.table_name,
            self.definition == other.definition,
            self.index == other.index,
            self.is_deferrable == other.is_deferrable,
            self.initially_deferred == other.initially_deferred,
        )
        return all(equalities)
