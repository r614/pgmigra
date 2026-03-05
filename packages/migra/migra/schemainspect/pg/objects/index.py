import textwrap

from ...inspected import Inspected, TableRelated


class InspectedIndex(Inspected, TableRelated):
    def __init__(
        self,
        name,
        schema,
        table_name,
        key_columns,
        key_options,
        num_att,
        is_unique,
        is_pk,
        is_exclusion,
        is_immediate,
        is_clustered,
        key_collations,
        key_expressions,
        partial_predicate,
        algorithm,
        definition=None,
        constraint=None,
        index_columns=None,
        included_columns=None,
    ):
        self.name = name
        self.schema = schema
        self.definition = definition
        self.table_name = table_name
        self.key_columns = key_columns
        self.key_options = key_options
        self.num_att = num_att
        self.is_unique = is_unique
        self.is_pk = is_pk
        self.is_exclusion = is_exclusion
        self.is_immediate = is_immediate
        self.is_clustered = is_clustered
        self.key_collations = key_collations
        self.key_expressions = key_expressions
        self.partial_predicate = partial_predicate
        self.algorithm = algorithm
        self.constraint = constraint
        self.index_columns = index_columns
        self.included_columns = included_columns

    @property
    def drop_statement(self):
        statement = f"drop index if exists {self.quoted_full_name};"

        if self.is_exclusion_constraint:
            return "select 1; " + textwrap.indent(statement, "-- ")
        return statement

    @property
    def create_statement(self):
        statement = f"{self.definition};"
        if self.is_exclusion_constraint:
            return "select 1; " + textwrap.indent(statement, "-- ")
        return statement

    @property
    def is_exclusion_constraint(self):
        return self.constraint and self.constraint.constraint_type == "EXCLUDE"

    def __eq__(self, other):
        equalities = (
            self.name == other.name,
            self.schema == other.schema,
            self.table_name == other.table_name,
            self.key_columns == other.key_columns,
            self.included_columns == other.included_columns,
            self.key_options == other.key_options,
            self.num_att == other.num_att,
            self.is_unique == other.is_unique,
            self.is_pk == other.is_pk,
            self.is_exclusion == other.is_exclusion,
            self.is_immediate == other.is_immediate,
            self.is_clustered == other.is_clustered,
            self.key_expressions == other.key_expressions,
            self.partial_predicate == other.partial_predicate,
            self.algorithm == other.algorithm,
        )
        return all(equalities)
