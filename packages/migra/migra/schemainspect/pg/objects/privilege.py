from ...inspected import Inspected
from ...misc import quoted_identifier
from ..registry import ObjectType, register


class InspectedPrivilege(Inspected):
    def __init__(self, object_type, schema, name, privilege, target_user):
        self.schema = schema
        self.object_type = object_type
        self.name = name
        self.privilege = privilege.lower()
        self.target_user = target_user

    @property
    def quoted_target_user(self):
        return quoted_identifier(self.target_user)

    @property
    def drop_statement(self):
        return f"revoke {self.privilege} on {self.object_type} {self.quoted_full_name} from {self.quoted_target_user};"

    @property
    def create_statement(self):
        return f"grant {self.privilege} on {self.object_type} {self.quoted_full_name} to {self.quoted_target_user};"

    def __eq__(self, other):
        equalities = (
            self.schema == other.schema,
            self.object_type == other.object_type,
            self.name == other.name,
            self.privilege == other.privilege,
            self.target_user == other.target_user,
        )
        return all(equalities)

    @property
    def key(self):
        return self.object_type, self.quoted_full_name, self.target_user, self.privilege


register(ObjectType(name="privileges", include_in_eq=False))
