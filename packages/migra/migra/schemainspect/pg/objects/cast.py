from ...inspected import Inspected
from ...misc import quoted_identifier


class InspectedCast(Inspected):
    def __init__(
        self,
        source_type,
        target_type,
        context,
        method,
        function_name,
        function_schema,
        function_args,
    ):
        self.name = target_type
        self.schema = ""
        self.source_type = source_type
        self.target_type = target_type
        self.context = context
        self.method = method
        self.function_name = function_name
        self.function_schema = function_schema
        self.function_args = function_args

    @property
    def key(self):
        return f"({self.source_type} AS {self.target_type})"

    @property
    def quoted_full_name(self):
        return self.key

    @property
    def _context_clause(self):
        if self.context == "a":
            return " AS ASSIGNMENT"
        if self.context == "i":
            return " AS IMPLICIT"
        return ""

    @property
    def create_statement(self):
        if self.method == "f":
            func = quoted_identifier(self.function_name, schema=self.function_schema)
            stmt = f"CREATE CAST ({self.source_type} AS {self.target_type}) WITH FUNCTION {func}({self.function_args})"
        elif self.method == "i":
            stmt = f"CREATE CAST ({self.source_type} AS {self.target_type}) WITH INOUT"
        else:
            stmt = f"CREATE CAST ({self.source_type} AS {self.target_type}) WITHOUT FUNCTION"
        stmt += self._context_clause
        return stmt + ";"

    @property
    def drop_statement(self):
        return f"DROP CAST ({self.source_type} AS {self.target_type});"

    def __eq__(self, other):
        return (
            self.source_type == other.source_type
            and self.target_type == other.target_type
            and self.context == other.context
            and self.method == other.method
            and self.function_name == other.function_name
            and self.function_args == other.function_args
        )
