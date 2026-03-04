from __future__ import annotations

from .misc import quoted_identifier


class Inspected:
    name: str
    schema: str
    dependents: list[str]
    dependent_on: list[str]

    @property
    def quoted_full_name(self) -> str:
        return quoted_identifier(self.name, schema=self.schema)

    @property
    def signature(self) -> str:
        return self.quoted_full_name

    @property
    def quoted_name(self) -> str:
        return quoted_identifier(self.name)

    @property
    def quoted_schema(self) -> str:
        return quoted_identifier(self.schema)

    @property
    def drop_statement(self) -> str:
        raise NotImplementedError

    @property
    def create_statement(self) -> str:
        raise NotImplementedError

    def alter_statements(self, other: Inspected) -> list[str]:
        raise NotImplementedError


class TableRelated:
    schema: str
    table_name: str

    @property
    def quoted_full_table_name(self):
        return f"{quoted_identifier(self.schema)}.{quoted_identifier(self.table_name)}"


class ColumnInfo:
    def __init__(
        self,
        name,
        dbtype,
        pytype,
        default=None,
        not_null=False,
        is_enum=False,
        enum=None,
        dbtypestr=None,
        collation=None,
        is_identity=False,
        is_identity_always=False,
        is_generated=False,
        is_inherited=False,
        can_drop_generated=False,
        generated_type=None,
    ):
        self.name = name or ""
        self.dbtype = dbtype
        self.dbtypestr = dbtypestr or dbtype
        self.pytype = pytype
        self.default = default or None
        self.not_null = not_null
        self.is_enum = is_enum
        self.enum = enum
        self.collation = collation
        self.is_identity = is_identity
        self.is_identity_always = is_identity_always
        self.is_generated = is_generated
        self.is_inherited = is_inherited
        self.can_drop_generated = can_drop_generated
        self.generated_type = generated_type

    def __eq__(self, other):
        return (
            self.name == other.name
            and self.dbtype == other.dbtype
            and self.dbtypestr == other.dbtypestr
            and self.default == other.default
            and self.not_null == other.not_null
            and self.enum == other.enum
            and self.collation == other.collation
            and self.is_identity == other.is_identity
            and self.is_identity_always == other.is_identity_always
            and self.is_generated == other.is_generated
            and self.is_inherited == other.is_inherited
            and self.generated_type == other.generated_type
        )

    def alter_clauses(self, other):

        # ordering:
        # identify must be dropped before notnull
        # notnull must be added before identity

        clauses = []

        notnull_changed = self.not_null != other.not_null
        notnull_added = notnull_changed and self.not_null
        notnull_dropped = notnull_changed and not self.not_null

        default_changed = self.default != other.default

        identity_changed = (
            self.is_identity != other.is_identity
            or self.is_identity_always != other.is_identity_always
        )

        type_or_collation_changed = (
            self.dbtypestr != other.dbtypestr or self.collation != other.collation
        )

        if default_changed:
            clauses.append(self.alter_default_clause_or_generated(other))

        if notnull_added:
            clauses.append(self.alter_not_null_clause)

        if identity_changed:
            clauses.append(self.alter_identity_clause(other))

        if notnull_dropped:
            clauses.append(self.alter_not_null_clause)

        if type_or_collation_changed:
            if self.is_enum and other.is_enum:
                clauses.append(self.alter_enum_type_clause)
            else:
                clauses.append(self.alter_data_type_clause)

        return clauses

    def change_enum_to_string_statement(self, table_name):
        if self.is_enum:
            return f"alter table {table_name} alter column {self.quoted_name} set data type varchar using {self.quoted_name}::varchar;"

        else:
            raise ValueError

    def change_string_to_enum_statement(self, table_name):
        if self.is_enum:
            return f"alter table {table_name} alter column {self.quoted_name} set data type {self.dbtypestr} using {self.quoted_name}::{self.dbtypestr};"
        else:
            raise ValueError

    def change_enum_statement(self, table_name):
        if self.is_enum:
            assert self.enum is not None
            return f"alter table {table_name} alter column {self.quoted_name} type {self.enum.quoted_full_name} using {self.quoted_name}::text::{self.enum.quoted_full_name};"
        else:
            raise ValueError

    def drop_default_statement(self, table_name):
        return f"alter table {table_name} alter column {self.quoted_name} drop default;"

    def add_default_statement(self, table_name):
        return f"alter table {table_name} alter column {self.quoted_name} set default {self.default};"

    def alter_table_statements(self, other, table_name):
        prefix = f"alter table {table_name}"
        return [f"{prefix} {c};" for c in self.alter_clauses(other)]

    @property
    def quoted_name(self):
        return quoted_identifier(self.name)

    @property
    def creation_clause(self):
        x = f"{self.quoted_name} {self.dbtypestr}"
        if self.is_identity:
            identity_type = "always" if self.is_identity_always else "by default"
            x += f" generated {identity_type} as identity"
        if self.not_null:
            x += " not null"
        if self.is_generated:
            gen_kind = "virtual" if self.generated_type == "v" else "stored"
            x += f" generated always as ({self.default}) {gen_kind}"
        elif self.default:
            x += f" default {self.default}"
        return x

    @property
    def add_column_clause(self):
        return f"add column {self.creation_clause}{self.collation_subclause}"

    @property
    def drop_column_clause(self):
        return f"drop column {self.quoted_name}"

    @property
    def alter_not_null_clause(self):
        keyword = "set" if self.not_null else "drop"
        return f"alter column {self.quoted_name} {keyword} not null"

    @property
    def alter_default_clause(self):
        if self.default:
            alter = f"alter column {self.quoted_name} set default {self.default}"
        else:
            alter = f"alter column {self.quoted_name} drop default"
        return alter

    def alter_default_clause_or_generated(self, other):
        if self.default:
            alter = f"alter column {self.quoted_name} set default {self.default}"
        elif other.is_generated and not self.is_generated:
            alter = f"alter column {self.quoted_name} drop expression"
        else:
            alter = f"alter column {self.quoted_name} drop default"
        return alter

    def alter_identity_clause(self, other):
        if self.is_identity:
            identity_type = "always" if self.is_identity_always else "by default"
            if other.is_identity:
                alter = f"alter column {self.quoted_name} set generated {identity_type}"
            else:
                alter = f"alter column {self.quoted_name} add generated {identity_type} as identity"
        else:
            alter = f"alter column {self.quoted_name} drop identity"
        return alter

    @property
    def collation_subclause(self):
        if self.collation:
            collate = f" collate {quoted_identifier(self.collation)}"
        else:
            collate = ""
        return collate

    @property
    def alter_data_type_clause(self):
        return f"alter column {self.quoted_name} set data type {self.dbtypestr}{self.collation_subclause} using {self.quoted_name}::{self.dbtypestr}"

    @property
    def alter_enum_type_clause(self):
        return f"alter column {self.quoted_name} set data type {self.dbtypestr}{self.collation_subclause} using {self.quoted_name}::text::{self.dbtypestr}"
