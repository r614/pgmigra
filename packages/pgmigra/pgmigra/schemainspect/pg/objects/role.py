from ...inspected import Inspected
from ...misc import quoted_identifier


class InspectedRole(Inspected):
    def __init__(
        self,
        name,
        superuser,
        inherit,
        createrole,
        createdb,
        login,
        replication,
        bypassrls,
        connlimit,
        member_of,
    ):
        self.name = name
        self.schema = ""
        self.superuser = superuser
        self.inherit = inherit
        self.createrole = createrole
        self.createdb = createdb
        self.login = login
        self.replication = replication
        self.bypassrls = bypassrls
        self.connlimit = connlimit
        self.member_of = member_of or []

    @property
    def quoted_full_name(self):
        return quoted_identifier(self.name)

    @property
    def options_clause(self):
        opts = []
        opts.append("SUPERUSER" if self.superuser else "NOSUPERUSER")
        opts.append("CREATEDB" if self.createdb else "NOCREATEDB")
        opts.append("CREATEROLE" if self.createrole else "NOCREATEROLE")
        opts.append("LOGIN" if self.login else "NOLOGIN")
        opts.append("REPLICATION" if self.replication else "NOREPLICATION")
        opts.append("BYPASSRLS" if self.bypassrls else "NOBYPASSRLS")
        opts.append("INHERIT" if self.inherit else "NOINHERIT")
        if self.connlimit >= 0:
            opts.append(f"CONNECTION LIMIT {self.connlimit}")
        return " ".join(opts)

    @property
    def create_statement(self):
        opts = self.options_clause
        stmt = f"create role {self.quoted_full_name}"
        if opts:
            stmt += f" {opts}"
        return stmt + ";"

    @property
    def drop_statement(self):
        return f"drop role if exists {self.quoted_full_name};"

    def alter_statements(self, other):
        stmts = []
        if self.options_clause != other.options_clause:
            opts = self.options_clause
            stmts.append(f"alter role {self.quoted_full_name} {opts};")
        old_memberships = set(other.member_of)
        new_memberships = set(self.member_of)
        for role in sorted(new_memberships - old_memberships):
            stmts.append(f"grant {quoted_identifier(role)} to {self.quoted_full_name};")
        for role in sorted(old_memberships - new_memberships):
            stmts.append(
                f"revoke {quoted_identifier(role)} from {self.quoted_full_name};"
            )
        return stmts

    def __eq__(self, other):
        return (
            self.name == other.name
            and self.superuser == other.superuser
            and self.inherit == other.inherit
            and self.createrole == other.createrole
            and self.createdb == other.createdb
            and self.login == other.login
            and self.replication == other.replication
            and self.bypassrls == other.bypassrls
            and self.connlimit == other.connlimit
            and sorted(self.member_of) == sorted(other.member_of)
        )
