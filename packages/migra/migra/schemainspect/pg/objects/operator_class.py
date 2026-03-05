from ...inspected import Inspected
from ...misc import quoted_identifier
from .operator import _qualified_operator


class InspectedOperatorClass(Inspected):
    def __init__(
        self,
        name,
        schema,
        access_method,
        is_default,
        type_name,
        family_name,
        family_schema,
        storage_type,
        operators=None,
        procs=None,
    ):
        self.name = name
        self.schema = schema
        self.access_method = access_method
        self.is_default = is_default
        self.type_name = type_name
        self.family_name = family_name
        self.family_schema = family_schema
        self.storage_type = storage_type
        self.operators = operators or []
        self.procs = procs or []

    @property
    def key(self):
        return f"{quoted_identifier(self.name, schema=self.schema)} USING {self.access_method}"

    @property
    def quoted_full_name(self):
        return self.key

    @property
    def create_statement(self):
        qn = quoted_identifier(self.name, schema=self.schema)
        stmt = f"CREATE OPERATOR CLASS {qn}"
        if self.is_default:
            stmt += " DEFAULT"
        stmt += f" FOR TYPE {self.type_name} USING {self.access_method}"
        family_qn = quoted_identifier(self.family_name, schema=self.family_schema)
        own_family_qn = quoted_identifier(self.name, schema=self.schema)
        if family_qn != own_family_qn:
            stmt += f" FAMILY {family_qn}"
        stmt += " AS"
        entries = []
        for op in self.operators:
            op_name = _qualified_operator(op["operator_name"], op["operator_schema"])
            entry = f"OPERATOR {op['strategy']} {op_name}"
            if op.get("left_type") and op.get("right_type"):
                entry += f" ({op['left_type']}, {op['right_type']})"
            entries.append(entry)
        for proc in self.procs:
            func = quoted_identifier(
                proc["function_name"], schema=proc["function_schema"]
            )
            entry = f"FUNCTION {proc['support_number']} {func}({proc['function_args']})"
            entries.append(entry)
        if (
            self.storage_type
            and self.storage_type != "-"
            and self.storage_type != self.type_name
        ):
            entries.append(f"STORAGE {self.storage_type}")
        if entries:
            stmt += "\n  " + ",\n  ".join(entries)
        return stmt + ";"

    @property
    def drop_statement(self):
        qn = quoted_identifier(self.name, schema=self.schema)
        return f"DROP OPERATOR CLASS {qn} USING {self.access_method};"

    def __eq__(self, other):
        return (
            self.name == other.name
            and self.schema == other.schema
            and self.access_method == other.access_method
            and self.is_default == other.is_default
            and self.type_name == other.type_name
            and self.family_name == other.family_name
            and self.family_schema == other.family_schema
            and self.storage_type == other.storage_type
            and self.operators == other.operators
            and self.procs == other.procs
        )
