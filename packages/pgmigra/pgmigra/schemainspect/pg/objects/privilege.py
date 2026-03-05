from ...inspected import Inspected
from ...misc import quoted_identifier


class InspectedPrivilege(Inspected):
    def __init__(self, object_type, schema, name, privilege, target_user, columns=None):
        self.schema = schema
        self.object_type = object_type
        self.name = name
        self.privilege = privilege.lower()
        self.target_user = target_user
        self.columns = sorted(columns) if columns else None

    @property
    def quoted_target_user(self):
        return quoted_identifier(self.target_user)

    @property
    def drop_statement(self):
        if self.columns:
            col_list = ", ".join(quoted_identifier(c) for c in self.columns)
            return f"revoke {self.privilege} ({col_list}) on table {self.quoted_full_name} from {self.quoted_target_user};"
        return f"revoke {self.privilege} on {self.object_type} {self.quoted_full_name} from {self.quoted_target_user};"

    @property
    def create_statement(self):
        if self.columns:
            col_list = ", ".join(quoted_identifier(c) for c in self.columns)
            return f"grant {self.privilege} ({col_list}) on table {self.quoted_full_name} to {self.quoted_target_user};"
        return f"grant {self.privilege} on {self.object_type} {self.quoted_full_name} to {self.quoted_target_user};"

    def __eq__(self, other):
        equalities = (
            self.schema == other.schema,
            self.object_type == other.object_type,
            self.name == other.name,
            self.privilege == other.privilege,
            self.target_user == other.target_user,
            self.columns == other.columns,
        )
        return all(equalities)

    @property
    def key(self):
        if self.columns:
            col_key = ",".join(self.columns)
            return (
                self.object_type,
                self.quoted_full_name,
                self.target_user,
                self.privilege,
                col_key,
            )
        return self.object_type, self.quoted_full_name, self.target_user, self.privilege
