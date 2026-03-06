from ...inspected import Inspected
from ...misc import quoted_identifier


class InspectedPublication(Inspected):
    def __init__(
        self,
        name,
        publish_all_tables,
        publish_insert,
        publish_update,
        publish_delete,
        publish_truncate,
        publish_via_partition_root,
        owner,
        tables,
        publish_generated_columns="none",
    ):
        self.name = name
        self.schema = ""
        self.publish_all_tables = publish_all_tables
        self.publish_insert = publish_insert
        self.publish_update = publish_update
        self.publish_delete = publish_delete
        self.publish_truncate = publish_truncate
        self.publish_via_partition_root = publish_via_partition_root
        self.publish_generated_columns = publish_generated_columns
        self.owner = owner
        self.tables = list(tables) if tables else []

    @property
    def quoted_full_name(self):
        return quoted_identifier(self.name)

    @property
    def _publish_options(self):
        ops = []
        if self.publish_insert:
            ops.append("insert")
        if self.publish_update:
            ops.append("update")
        if self.publish_delete:
            ops.append("delete")
        if self.publish_truncate:
            ops.append("truncate")
        return ops

    @property
    def _with_clause(self):
        parts = []
        publish = ", ".join(self._publish_options)
        default_publish = "insert, update, delete, truncate"
        if publish != default_publish:
            parts.append(f"publish = '{publish}'")
        if self.publish_via_partition_root:
            parts.append("publish_via_partition_root = true")
        if self.publish_generated_columns != "none":
            parts.append(
                f"publish_generated_columns = '{self.publish_generated_columns}'"
            )
        if parts:
            return " WITH (" + ", ".join(parts) + ")"
        return ""

    @property
    def create_statement(self):
        stmt = f"CREATE PUBLICATION {self.quoted_full_name}"
        if self.publish_all_tables:
            stmt += " FOR ALL TABLES"
        elif self.tables:
            stmt += " FOR TABLE " + ", ".join(self.tables)
        stmt += self._with_clause
        return stmt + ";"

    @property
    def drop_statement(self):
        return f"DROP PUBLICATION {self.quoted_full_name};"

    def alter_statements(self, other):
        stmts = []
        if (
            self._publish_options != other._publish_options
            or self.publish_via_partition_root != other.publish_via_partition_root
            or self.publish_generated_columns != other.publish_generated_columns
        ):
            parts = []
            publish = ", ".join(self._publish_options)
            parts.append(f"publish = '{publish}'")
            if self.publish_via_partition_root:
                parts.append("publish_via_partition_root = true")
            else:
                parts.append("publish_via_partition_root = false")
            if self.publish_generated_columns != other.publish_generated_columns:
                parts.append(
                    f"publish_generated_columns = '{self.publish_generated_columns}'"
                )
            stmts.append(
                f"ALTER PUBLICATION {self.quoted_full_name} SET ({', '.join(parts)});"
            )
        if not self.publish_all_tables and sorted(self.tables) != sorted(other.tables):
            if self.tables:
                stmts.append(
                    f"ALTER PUBLICATION {self.quoted_full_name} SET TABLE {', '.join(self.tables)};"
                )
            else:
                for t in other.tables:
                    stmts.append(
                        f"ALTER PUBLICATION {self.quoted_full_name} DROP TABLE {t};"
                    )
        if self.owner != other.owner:
            stmts.append(
                f"ALTER PUBLICATION {self.quoted_full_name} OWNER TO {quoted_identifier(self.owner)};"
            )
        return stmts

    def __eq__(self, other):
        return (
            self.name == other.name
            and self.publish_all_tables == other.publish_all_tables
            and self.publish_insert == other.publish_insert
            and self.publish_update == other.publish_update
            and self.publish_delete == other.publish_delete
            and self.publish_truncate == other.publish_truncate
            and self.publish_via_partition_root == other.publish_via_partition_root
            and self.publish_generated_columns == other.publish_generated_columns
            and self.owner == other.owner
            and sorted(self.tables) == sorted(other.tables)
        )
