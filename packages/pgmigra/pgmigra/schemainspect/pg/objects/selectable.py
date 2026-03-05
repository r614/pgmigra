from ...inspected import Inspected


def _format_ft_options(options_str):
    parts = []
    for opt in options_str.split(", "):
        if "=" in opt:
            k, v = opt.split("=", 1)
            parts.append(f"{k} '{v}'")
        else:
            parts.append(opt)
    return ", ".join(parts)


class InspectedSelectable(Inspected):
    def __init__(
        self,
        name,
        schema,
        columns,
        inputs=None,
        definition=None,
        dependent_on=None,
        dependents=None,
        comment=None,
        relationtype="unknown",
        parent_table=None,
        partition_def=None,
        partition_spec=None,
        rowsecurity=False,
        forcerowsecurity=False,
        persistence=None,
        owner=None,
        ft_server_name=None,
        ft_options=None,
    ):
        self.name = name
        self.schema = schema
        self.inputs = inputs or []
        self.columns = columns
        self.definition = definition
        self.relationtype = relationtype
        self.dependent_on = dependent_on or []
        self.dependents = dependents or []
        self.dependent_on_all = []
        self.dependents_all = []
        self.constraints = {}
        self.indexes = {}
        self.comment = comment
        self.parent_table = parent_table
        self.partition_def = partition_def
        self.partition_spec = partition_spec
        self.rowsecurity = rowsecurity
        self.forcerowsecurity = forcerowsecurity
        self.persistence = persistence
        self.owner = owner
        self.ft_server_name = ft_server_name
        self.ft_options = ft_options

    def __eq__(self, other):
        equalities = (
            type(self) == type(other),
            self.relationtype == other.relationtype,
            self.name == other.name,
            self.schema == other.schema,
            dict(self.columns) == dict(other.columns),
            self.inputs == other.inputs,
            self.definition == other.definition,
            self.parent_table == other.parent_table,
            self.partition_def == other.partition_def,
            self.rowsecurity == other.rowsecurity,
            self.persistence == other.persistence,
            self.ft_server_name == other.ft_server_name,
            self.ft_options == other.ft_options,
        )
        return all(equalities)

    def has_compatible_columns(self, other):
        def names_and_types(cols):
            return [(k, c.dbtype) for k, c in cols.items()]

        items = names_and_types(self.columns)

        if self.relationtype != "f":
            old_arg_count = len(other.columns)
            items = items[:old_arg_count]

        return items == names_and_types(other.columns)

    def can_replace(self, other):
        if not (self.relationtype in ("v", "f") or self.is_table):
            return False

        if self.signature != other.signature:
            return False

        if self.relationtype != other.relationtype:
            return False

        return self.has_compatible_columns(other)

    @property
    def persistence_modifier(self):
        if self.persistence == "t":
            return "temporary "
        elif self.persistence == "u":
            return "unlogged "
        else:
            return ""

    @property
    def is_unlogged(self):
        return self.persistence == "u"

    @property
    def create_statement(self):
        n = self.quoted_full_name
        if self.relationtype in ("r", "p"):
            if not self.is_partitioning_child_table:
                colspec = ",\n".join(
                    "    " + c.creation_clause for c in self.columns.values()
                )
                if colspec:
                    colspec = "\n" + colspec

                if self.partition_def:
                    partition_key = " partition by " + self.partition_def
                    inherits_clause = ""
                elif self.parent_table:
                    inherits_clause = f" inherits ({self.parent_table})"
                    partition_key = ""
                else:
                    partition_key = ""
                    inherits_clause = ""

                create_statement = f"create {self.persistence_modifier}table {n} ({colspec}\n){partition_key}{inherits_clause};\n"
            else:
                create_statement = f"create {self.persistence_modifier}table {n} partition of {self.parent_table} {self.partition_def};\n"
        elif self.relationtype == "v":
            create_statement = f"create or replace view {n} as {self.definition}\n"
        elif self.relationtype == "m":
            create_statement = f"create materialized view {n} as {self.definition}\n"
        elif self.relationtype == "c":
            colspec = ", ".join(c.creation_clause for c in self.columns.values())
            create_statement = f"create type {n} as ({colspec});"
        elif self.relationtype == "ft":
            colspec = ",\n".join(
                "    " + c.creation_clause for c in self.columns.values()
            )
            if colspec:
                colspec = "\n" + colspec
            server_clause = (
                f" server {self.ft_server_name}" if self.ft_server_name else ""
            )
            options_clause = ""
            if self.ft_options:
                opts = _format_ft_options(self.ft_options)
                options_clause = f" options ({opts})"
            create_statement = f"create foreign table {n} ({colspec}\n){server_clause}{options_clause};\n"
        else:
            raise NotImplementedError  # pragma: no cover
        return create_statement

    @property
    def drop_statement(self):
        n = self.quoted_full_name
        if self.relationtype in ("r", "p"):
            drop_statement = f"drop table {n};"
        elif self.relationtype == "v":
            drop_statement = f"drop view if exists {n};"
        elif self.relationtype == "m":
            drop_statement = f"drop materialized view if exists {n};"
        elif self.relationtype == "c":
            drop_statement = f"drop type {n};"
        elif self.relationtype == "ft":
            drop_statement = f"drop foreign table if exists {n};"
        else:
            raise NotImplementedError  # pragma: no cover

        return drop_statement

    def alter_table_statement(self, clause):
        if self.is_alterable:
            alter = f"alter table {self.quoted_full_name} {clause};"
        else:
            raise NotImplementedError  # pragma: no cover

        return alter

    @property
    def is_partitioned(self):
        return self.relationtype == "p"

    @property
    def is_inheritance_child_table(self):
        return bool(self.parent_table) and not self.partition_def

    @property
    def is_table(self):
        return self.relationtype in ("p", "r")

    @property
    def is_alterable(self):
        return self.is_table and (
            not self.parent_table or self.is_inheritance_child_table
        )

    @property
    def contains_data(self):
        return bool(
            self.relationtype == "r" and (self.parent_table or not self.partition_def)
        )

    @property
    def is_partitioning_child_table(self):
        return bool(
            self.relationtype == "r" and self.parent_table and self.partition_def
        )

    @property
    def uses_partitioning(self):
        return self.is_partitioning_child_table or self.is_partitioned

    @property
    def attach_statement(self):
        if self.parent_table:
            if self.partition_def:
                return f"alter table {self.quoted_full_name} attach partition {self.parent_table} {self.partition_spec};"
            else:
                return (
                    f"alter table {self.quoted_full_name} inherit {self.parent_table}"
                )

    @property
    def detach_statement(self):
        if self.parent_table:
            if self.partition_def:
                return f"alter table {self.parent_table} detach partition {self.quoted_full_name};"
            else:
                return f"alter table {self.quoted_full_name} no inherit {self.parent_table}"

    def attach_detach_statements(self, before):
        slist = []
        if self.parent_table != before.parent_table:
            if before.parent_table:
                slist.append(before.detach_statement)
            if self.parent_table:
                slist.append(self.attach_statement)
        return slist

    @property
    def alter_rls_clause(self):
        keyword = "enable" if self.rowsecurity else "disable"
        return f"{keyword} row level security"

    @property
    def alter_rls_statement(self):
        return self.alter_table_statement(self.alter_rls_clause)

    @property
    def alter_unlogged_statement(self):
        keyword = "unlogged" if self.is_unlogged else "logged"
        return self.alter_table_statement(f"set {keyword}")


class InspectedFunction(InspectedSelectable):
    def __init__(
        self,
        name,
        schema,
        columns,
        inputs,
        definition,
        volatility,
        strictness,
        security_type,
        identity_arguments,
        result_string,
        language,
        full_definition,
        comment,
        returntype,
        kind,
    ):
        self.identity_arguments = identity_arguments
        self.result_string = result_string
        self.language = language
        self.volatility = volatility
        self.strictness = strictness
        self.security_type = security_type
        self.full_definition = full_definition
        self.returntype = returntype
        self.kind = kind

        super().__init__(
            name=name,
            schema=schema,
            columns=columns,
            inputs=inputs,
            definition=definition,
            relationtype="f",
            comment=comment,
        )

    @property
    def returntype_is_table(self):
        if self.returntype:
            return "." in self.returntype

    @property
    def signature(self):
        return f"{self.quoted_full_name}({self.identity_arguments})"

    @property
    def create_statement(self):
        return self.full_definition + ";"

    @property
    def thing(self):
        kinds = dict(f="function", p="procedure", a="aggregate", w="window function")
        return kinds[self.kind]

    @property
    def drop_statement(self):
        return f"drop {self.thing} if exists {self.signature};"

    def __eq__(self, other):
        return (
            self.signature == other.signature
            and self.result_string == other.result_string
            and self.definition == other.definition
            and self.language == other.language
            and self.volatility == other.volatility
            and self.strictness == other.strictness
            and self.security_type == other.security_type
            and self.kind == other.kind
        )

    def can_replace(self, other):
        if self.signature != other.signature:
            return False
        if self.relationtype != other.relationtype:
            return False
        if self.result_string != other.result_string:
            return False
        return self.has_compatible_columns(other)
