from ...inspected import Inspected


class InspectedCollation(Inspected):
    def __init__(self, name, schema, provider, encoding, lc_collate, lc_ctype, version):
        self.name = name
        self.schema = schema
        self.provider = provider
        self.lc_collate = lc_collate
        self.lc_ctype = lc_ctype
        self.encoding = encoding
        self.version = version

    @property
    def locale(self):
        return self.lc_collate

    @property
    def drop_statement(self):
        return f"drop collation if exists {self.quoted_full_name};"

    @property
    def create_statement(self):
        return f"create collation if not exists {self.quoted_full_name} (provider = '{self.provider}', locale = '{self.locale}');"

    def __eq__(self, other):

        equalities = (
            self.name == other.name,
            self.schema == other.schema,
            self.provider == other.provider,
            self.locale == other.locale,
        )
        return all(equalities)
