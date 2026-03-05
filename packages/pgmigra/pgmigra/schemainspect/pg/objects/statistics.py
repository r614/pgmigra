from ...inspected import Inspected


class InspectedStatistics(Inspected):
    def __init__(self, name, schema, table_schema, table_name, stattarget, definition):
        self.name = name
        self.schema = schema
        self.table_schema = table_schema
        self.table_name = table_name
        self.stattarget = stattarget
        self.definition = definition

    @property
    def _has_custom_stattarget(self):
        return self.stattarget is not None and self.stattarget != -1

    @property
    def create_statement(self):
        stmt = self.definition + ";"
        if self._has_custom_stattarget:
            stmt += f"\nALTER STATISTICS {self.quoted_full_name} SET STATISTICS {self.stattarget};"
        return stmt

    @property
    def drop_statement(self):
        return f"DROP STATISTICS {self.quoted_full_name};"

    def __eq__(self, other):
        return (
            self.name == other.name
            and self.schema == other.schema
            and self.table_schema == other.table_schema
            and self.table_name == other.table_name
            and self._has_custom_stattarget == other._has_custom_stattarget
            and (not self._has_custom_stattarget or self.stattarget == other.stattarget)
            and self.definition == other.definition
        )
