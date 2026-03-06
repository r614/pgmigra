from ...inspected import Inspected
from ...misc import quoted_identifier


def _qualified_operator(name, schema=None):
    if schema:
        return f"{quoted_identifier(schema)}.{name}"
    return name


class InspectedOperator(Inspected):
    def __init__(
        self,
        name,
        schema,
        left_type,
        right_type,
        result_type,
        function_name,
        function_schema,
        function_args,
        commutator_name,
        commutator_schema,
        negator_name,
        negator_schema,
        can_hash,
        can_merge,
    ):
        self.name = name
        self.schema = schema
        self.left_type = left_type
        self.right_type = right_type
        self.result_type = result_type
        self.function_name = function_name
        self.function_schema = function_schema
        self.function_args = function_args
        self.commutator_name = commutator_name
        self.commutator_schema = commutator_schema
        self.negator_name = negator_name
        self.negator_schema = negator_schema
        self.can_hash = can_hash
        self.can_merge = can_merge

    @property
    def _arg_signature(self):
        left = self.left_type if self.left_type and self.left_type != "-" else "NONE"
        right = (
            self.right_type if self.right_type and self.right_type != "-" else "NONE"
        )
        return f"{left}, {right}"

    @property
    def _qualified_name(self):
        return _qualified_operator(self.name, self.schema)

    @property
    def key(self):
        return f"{self._qualified_name}({self._arg_signature})"

    @property
    def quoted_full_name(self):
        return self.key

    @property
    def create_statement(self):
        parts = []
        func = quoted_identifier(self.function_name, schema=self.function_schema)
        parts.append(f"FUNCTION = {func}")
        if self.left_type and self.left_type != "-":
            parts.append(f"LEFTARG = {self.left_type}")
        if self.right_type and self.right_type != "-":
            parts.append(f"RIGHTARG = {self.right_type}")
        if self.commutator_name:
            comm = _qualified_operator(self.commutator_name, self.commutator_schema)
            parts.append(f"COMMUTATOR = OPERATOR({comm})")
        if self.negator_name:
            neg = _qualified_operator(self.negator_name, self.negator_schema)
            parts.append(f"NEGATOR = OPERATOR({neg})")
        if self.can_hash:
            parts.append("HASHES")
        if self.can_merge:
            parts.append("MERGES")
        body = ",\n  ".join(parts)
        return f"CREATE OPERATOR {self._qualified_name} (\n  {body}\n);"

    @property
    def drop_statement(self):
        return f"DROP OPERATOR {self._qualified_name} ({self._arg_signature});"

    def __eq__(self, other):
        return (
            self.name == other.name
            and self.schema == other.schema
            and self.left_type == other.left_type
            and self.right_type == other.right_type
            and self.result_type == other.result_type
            and self.function_name == other.function_name
            and self.function_schema == other.function_schema
            and self.function_args == other.function_args
            and self.commutator_name == other.commutator_name
            and self.commutator_schema == other.commutator_schema
            and self.negator_name == other.negator_name
            and self.negator_schema == other.negator_schema
            and self.can_hash == other.can_hash
            and self.can_merge == other.can_merge
        )
