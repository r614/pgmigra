from graphlib import TopologicalSorter
from importlib import resources
from io import StringIO
from itertools import groupby

import yaml

from ..inspected import ColumnInfo, Inspected
from ..inspector import DBInspector
from ..misc import quoted_identifier
from .objects import (
    InspectedCollation,
    InspectedComment,
    InspectedConstraint,
    InspectedDomain,
    InspectedEnum,
    InspectedExtension,
    InspectedFunction,
    InspectedIndex,
    InspectedPrivilege,
    InspectedRangeType,
    InspectedRole,
    InspectedRowPolicy,
    InspectedSchema,
    InspectedSelectable,
    InspectedSequence,
    InspectedTrigger,
    InspectedType,
)
from .registry import COMPOUND_PROPS, REGISTRY

_FILTERABLE_PROPS = [
    name for name, ot in REGISTRY.items() if ot.schema_filterable
] + COMPOUND_PROPS

assert __package__ is not None
_sql_files = resources.files(__package__).joinpath("sql")


def _read_sql(name: str) -> str:
    return _sql_files.joinpath(name).read_text(encoding="utf-8")


ALL_RELATIONS_QUERY = _read_sql("relations.sql")
ALL_RELATIONS_QUERY_9 = _read_sql("relations9.sql")
SCHEMAS_QUERY = _read_sql("schemas.sql")
INDEXES_QUERY = _read_sql("indexes.sql")
SEQUENCES_QUERY = _read_sql("sequences.sql")
CONSTRAINTS_QUERY = _read_sql("constraints.sql")
FUNCTIONS_QUERY = _read_sql("functions.sql")
TYPES_QUERY = _read_sql("types.sql")
DOMAINS_QUERY = _read_sql("domains.sql")
EXTENSIONS_QUERY = _read_sql("extensions.sql")
ENUMS_QUERY = _read_sql("enums.sql")
DEPS_QUERY = _read_sql("deps.sql")
PRIVILEGES_QUERY = _read_sql("privileges.sql")
TRIGGERS_QUERY = _read_sql("triggers.sql")
COLLATIONS_QUERY = _read_sql("collations.sql")
COLLATIONS_QUERY_9 = _read_sql("collations9.sql")
RLSPOLICIES_QUERY = _read_sql("rlspolicies.sql")
COMMENTS_QUERY = _read_sql("comments.sql")
ROLES_QUERY = _read_sql("roles.sql")
RANGE_TYPES_QUERY = _read_sql("range_types.sql")


class PostgreSQL(DBInspector):
    def __init__(self, c, include_internal=False):
        self.pg_version = c.info.server_version // 10000

        def processed(q):
            if not include_internal:
                q = q.replace("-- SKIP_INTERNAL", "")
            if self.pg_version >= 11:
                q = q.replace("-- 11_AND_LATER", "")
            else:
                q = q.replace("-- 10_AND_EARLIER", "")
            q = q.replace(r"\:", ":")
            return q

        if self.pg_version <= 9:
            self.ALL_RELATIONS_QUERY = processed(ALL_RELATIONS_QUERY_9)
            self.COLLATIONS_QUERY = processed(COLLATIONS_QUERY_9)
            self.RLSPOLICIES_QUERY = None
        else:
            all_relations_query = ALL_RELATIONS_QUERY

            if self.pg_version >= 12:
                replace = "-- 12_ONLY"
            else:
                replace = "-- PRE_12"

            all_relations_query = all_relations_query.replace(replace, "")
            self.ALL_RELATIONS_QUERY = processed(all_relations_query)
            collations_query = COLLATIONS_QUERY
            if self.pg_version >= 17:
                collations_query = collations_query.replace("-- 17_AND_LATER", "")
            elif self.pg_version >= 15:
                collations_query = collations_query.replace("-- 15_TO_16", "")
            else:
                collations_query = collations_query.replace("-- PRE_15", "")
            self.COLLATIONS_QUERY = processed(collations_query)
            self.RLSPOLICIES_QUERY = processed(RLSPOLICIES_QUERY)

        self.INDEXES_QUERY = processed(INDEXES_QUERY)
        self.SEQUENCES_QUERY = processed(SEQUENCES_QUERY)
        self.CONSTRAINTS_QUERY = processed(CONSTRAINTS_QUERY)
        self.FUNCTIONS_QUERY = processed(FUNCTIONS_QUERY)
        self.TYPES_QUERY = processed(TYPES_QUERY)
        self.DOMAINS_QUERY = processed(DOMAINS_QUERY)
        self.EXTENSIONS_QUERY = processed(EXTENSIONS_QUERY)
        self.ENUMS_QUERY = processed(ENUMS_QUERY)
        self.DEPS_QUERY = processed(DEPS_QUERY)
        self.SCHEMAS_QUERY = processed(SCHEMAS_QUERY)
        self.PRIVILEGES_QUERY = processed(PRIVILEGES_QUERY)
        self.TRIGGERS_QUERY = processed(TRIGGERS_QUERY)
        self.COMMENTS_QUERY = processed(COMMENTS_QUERY)
        self.ROLES_QUERY = processed(ROLES_QUERY)
        self.RANGE_TYPES_QUERY = processed(RANGE_TYPES_QUERY)

        super().__init__(c, include_internal)

    def execute(self, q):
        return self.c.execute(q).fetchall()

    def load_all(self):
        self.load_schemas()
        self.load_all_relations()
        self.load_functions()
        self.selectables = {}
        self.selectables.update(self.relations)
        self.selectables.update(self.composite_types)
        self.selectables.update(self.functions)

        self.load_privileges()
        self.load_triggers()
        self.load_collations()
        self.load_rlspolicies()
        self.load_types()
        self.load_domains()
        self.load_range_types()
        self.load_comments()
        self.load_roles()

        self.load_deps()
        self.load_deps_all()

    def load_schemas(self):
        q = self.execute(self.SCHEMAS_QUERY)
        schemas = [InspectedSchema(schema=each.schema) for each in q]
        self.schemas = {schema.schema: schema for schema in schemas}

    def load_rlspolicies(self):
        if self.pg_version <= 9:
            self.rlspolicies = {}
            return

        q = self.execute(self.RLSPOLICIES_QUERY)

        rlspolicies = [
            InspectedRowPolicy(
                name=p.name,
                schema=p.schema,
                table_name=p.table_name,
                commandtype=p.commandtype,
                permissive=p.permissive,
                roles=p.roles,
                qual=p.qual,
                withcheck=p.withcheck,
            )
            for p in q
        ]

        self.rlspolicies = {p.key: p for p in rlspolicies}

    def load_collations(self):
        q = self.execute(self.COLLATIONS_QUERY)
        collations = [
            InspectedCollation(
                schema=i.schema,
                name=i.name,
                provider=i.provider,
                encoding=i.encoding,
                lc_collate=i.lc_collate,
                lc_ctype=i.lc_ctype,
                version=i.version,
            )
            for i in q
        ]
        self.collations = {i.quoted_full_name: i for i in collations}

    def load_privileges(self):
        q = self.execute(self.PRIVILEGES_QUERY)
        privileges = [
            InspectedPrivilege(
                object_type=i.object_type,
                schema=i.schema,
                name=i.name,
                privilege=i.privilege,
                target_user=i.user,
            )
            for i in q
        ]
        self.privileges = {i.key: i for i in privileges}

    def load_deps(self):
        q = self.execute(self.DEPS_QUERY)

        self.deps = list(q)

        for dep in self.deps:
            x = quoted_identifier(dep.name, dep.schema, dep.identity_arguments)
            x_dependent_on = quoted_identifier(
                dep.name_dependent_on,
                dep.schema_dependent_on,
                dep.identity_arguments_dependent_on,
            )
            self.selectables[x].dependent_on.append(x_dependent_on)
            self.selectables[x].dependent_on.sort()

            try:
                self.selectables[x_dependent_on].dependents.append(x)
                self.selectables[x_dependent_on].dependents.sort()
            except LookupError:
                pass

        for k, t in self.triggers.items():
            for dep_name in t.dependent_on:
                try:
                    dependency = self.selectables[dep_name]
                except KeyError:
                    continue
                dependency.dependents.append(k)

        for k, r in self.relations.items():
            for kc, c in r.columns.items():
                if c.is_enum:
                    e_sig = c.enum.signature

                    if e_sig in self.enums:
                        r.dependent_on.append(e_sig)
                        c.enum.dependents.append(k)

            if r.parent_table:
                pt = self.relations[r.parent_table]
                r.dependent_on.append(r.parent_table)
                pt.dependents.append(r.signature)

    def get_dependency_by_signature(self, signature):
        things = [self.selectables, self.enums, self.triggers]

        for thing in things:
            try:
                return thing[signature]
            except KeyError:
                continue

    def load_deps_all(self):
        def get_related_for_item(item, att):
            related = [self.get_dependency_by_signature(_) for _ in getattr(item, att)]
            return [item.signature] + [
                _ for d in related for _ in get_related_for_item(d, att)
            ]

        for k, x in self.selectables.items():
            d_all = get_related_for_item(x, "dependent_on")[1:]
            d_all.sort()
            x.dependent_on_all = d_all
            d_all = get_related_for_item(x, "dependents")[1:]
            d_all.sort()
            x.dependents_all = d_all

    def dependency_order(
        self,
        drop_order=False,
        selectables=True,
        triggers=True,
        enums=True,
        include_fk_deps=False,
    ):
        graph, things = {}, {}

        if enums:
            things.update(self.enums)
        if selectables:
            things.update(self.selectables)
        if triggers:
            things.update(self.triggers)

        for k, x in things.items():
            dependent_on = list(x.dependent_on)

            if k in self.tables and x.parent_table:
                dependent_on.append(x.parent_table)

            graph[k] = dependent_on

        if include_fk_deps:
            fk_deps = {}

            for k, x in self.constraints.items():
                if x.is_fk:
                    t, other_t = (
                        x.quoted_full_table_name,
                        x.quoted_full_foreign_table_name,
                    )
                    fk_deps[t] = [other_t]

            graph.update(fk_deps)

        ts = TopologicalSorter(graph)

        ordering = []

        ts.prepare()

        while ts.is_active():
            items = ts.get_ready()

            itemslist = list(items)

            ordering += itemslist
            ts.done(*items)

        if drop_order:
            ordering.reverse()
        return ordering

    @property
    def partitioned_tables(self):
        return {k: v for k, v in self.tables.items() if v.is_partitioned}

    @property
    def alterable_tables(self):
        return {k: v for k, v in self.tables.items() if v.is_alterable}

    @property
    def data_tables(self):
        return {k: v for k, v in self.tables.items() if v.contains_data}

    @property
    def partitioning_child_tables(self):
        return {k: v for k, v in self.tables.items() if v.is_partitioning_child_table}

    @property
    def tables_using_partitioning(self):
        return {k: v for k, v in self.tables.items() if v.uses_partitioning}

    @property
    def tables_not_using_partitioning(self):
        return {k: v for k, v in self.tables.items() if not v.uses_partitioning}

    def load_all_relations(self):
        self.tables = {}
        self.views = {}
        self.materialized_views = {}
        self.composite_types = {}

        q = self.execute(self.ENUMS_QUERY)
        enumlist = [
            InspectedEnum(
                name=i.name,
                schema=i.schema,
                elements=i.elements,
                pg_version=self.pg_version,
            )
            for i in q
        ]
        self.enums = {i.quoted_full_name: i for i in enumlist}
        q = self.execute(self.ALL_RELATIONS_QUERY)

        for _, g in groupby(q, lambda x: (x.relationtype, x.schema, x.name)):
            clist = list(g)
            f = clist[0]

            def get_enum(name, schema):
                if not name and not schema:
                    return None

                quoted_full_name = (
                    f"{quoted_identifier(schema)}.{quoted_identifier(name)}"
                )

                return self.enums.get(quoted_full_name)

            columns = [
                ColumnInfo(
                    name=c.attname,
                    dbtype=c.datatype,
                    dbtypestr=c.datatypestring,
                    pytype=self.to_pytype(c.datatype),
                    default=c.defaultdef,
                    not_null=c.not_null,
                    is_enum=c.is_enum,
                    enum=get_enum(c.enum_name, c.enum_schema),
                    collation=c.collation,
                    is_identity=c.is_identity,
                    is_identity_always=c.is_identity_always,
                    is_generated=c.is_generated,
                    can_drop_generated=self.pg_version >= 13,
                    generated_type=getattr(c, "generated_type", None),
                )
                for c in clist
                if c.position_number
            ]

            s = InspectedSelectable(
                name=f.name,
                schema=f.schema,
                columns={c.name: c for c in columns},
                relationtype=f.relationtype,
                definition=f.definition,
                comment=f.comment,
                parent_table=f.parent_table,
                partition_def=f.partition_def,
                rowsecurity=f.rowsecurity,
                forcerowsecurity=f.forcerowsecurity,
                persistence=f.persistence,
                owner=f.owner,
            )
            RELATIONTYPES = {
                "r": "tables",
                "v": "views",
                "m": "materialized_views",
                "c": "composite_types",
                "p": "tables",
            }
            att = getattr(self, RELATIONTYPES[f.relationtype])
            att[s.quoted_full_name] = s

        for k, t in self.tables.items():
            if t.is_inheritance_child_table:
                parent_table = self.tables[t.parent_table]
                for cname, c in t.columns.items():
                    if cname in parent_table.columns:
                        c.is_inherited = True

        self.relations = {}
        for x in (self.tables, self.views, self.materialized_views):
            self.relations.update(x)
        q = self.execute(self.INDEXES_QUERY)
        indexlist = [
            InspectedIndex(
                name=i.name,
                schema=i.schema,
                definition=i.definition,
                table_name=i.table_name,
                key_columns=i.key_columns,
                index_columns=i.index_columns,
                included_columns=i.included_columns,
                key_options=i.key_options,
                num_att=i.num_att,
                is_unique=i.is_unique,
                is_pk=i.is_pk,
                is_exclusion=i.is_exclusion,
                is_immediate=i.is_immediate,
                is_clustered=i.is_clustered,
                key_collations=i.key_collations,
                key_expressions=i.key_expressions,
                partial_predicate=i.partial_predicate,
                algorithm=i.algorithm,
            )
            for i in q
        ]
        self.indexes = {i.quoted_full_name: i for i in indexlist}
        q = self.execute(self.SEQUENCES_QUERY)

        sequencelist = [
            InspectedSequence(
                name=i.name,
                schema=i.schema,
                table_name=i.table_name,
                column_name=i.column_name,
            )
            for i in q
        ]
        self.sequences = {i.quoted_full_name: i for i in sequencelist}
        q = self.execute(self.CONSTRAINTS_QUERY)

        constraintlist = []

        for i in q:
            constraint = InspectedConstraint(
                name=i.name,
                schema=i.schema,
                constraint_type=i.constraint_type,
                table_name=i.table_name,
                definition=i.definition,
                index=getattr(i, "index"),
                is_fk=i.is_fk,
                is_deferrable=i.is_deferrable,
                initially_deferred=i.initially_deferred,
            )
            if constraint.index:
                index_name = quoted_identifier(constraint.index, schema=i.schema)
                index = self.indexes[index_name]
                index.constraint = constraint
                constraint.index = index

            if constraint.is_fk:
                constraint.quoted_full_foreign_table_name = quoted_identifier(
                    i.foreign_table_name, schema=i.foreign_table_schema
                )
                constraint.fk_columns_foreign = i.fk_columns_foreign
                constraint.fk_columns_local = i.fk_columns_local

            constraintlist.append(constraint)

        self.constraints = {i.quoted_full_name: i for i in constraintlist}

        q = self.execute(self.EXTENSIONS_QUERY)
        extensionlist = [
            InspectedExtension(name=i.name, schema=i.schema, version=i.version)
            for i in q
        ]
        self.extensions = {i.name: i for i in extensionlist}
        for each in self.indexes.values():
            t = each.quoted_full_table_name
            n = each.quoted_full_name
            self.relations[t].indexes[n] = each
        for each in self.constraints.values():
            t = each.quoted_full_table_name
            n = each.quoted_full_name
            self.relations[t].constraints[n] = each

    @property
    def extensions_without_versions(self):
        return {k: v.unversioned_copy() for k, v in self.extensions.items()}

    def load_functions(self):
        self.functions = {}
        q = self.execute(self.FUNCTIONS_QUERY)
        for _, g in groupby(q, lambda x: (x.schema, x.name, x.identity_arguments)):
            clist = list(g)
            f = clist[0]
            outs = [c for c in clist if c.parameter_mode == "OUT"]
            if outs:
                columns = [
                    ColumnInfo(
                        name=c.parameter_name,
                        dbtype=c.data_type,
                        pytype=self.to_pytype(c.data_type),
                    )
                    for c in outs
                ]
            else:
                columns = [
                    ColumnInfo(
                        name=f.name,
                        dbtype=f.data_type,
                        pytype=self.to_pytype(f.returntype),
                        default=f.parameter_default,
                    )
                ]
            plist = [
                ColumnInfo(
                    name=c.parameter_name,
                    dbtype=c.data_type,
                    pytype=self.to_pytype(c.data_type),
                    default=c.parameter_default,
                )
                for c in clist
                if c.parameter_mode == "IN"
            ]
            s = InspectedFunction(
                schema=f.schema,
                name=f.name,
                columns={c.name: c for c in columns},
                inputs=plist,
                identity_arguments=f.identity_arguments,
                result_string=f.result_string,
                language=f.language,
                definition=f.definition,
                strictness=f.strictness,
                security_type=f.security_type,
                volatility=f.volatility,
                full_definition=f.full_definition,
                comment=f.comment,
                returntype=f.returntype,
                kind=f.kind,
            )

            identity_arguments = f"({s.identity_arguments})"
            self.functions[s.quoted_full_name + identity_arguments] = s

    def load_triggers(self):
        q = self.execute(self.TRIGGERS_QUERY)
        triggers = [
            InspectedTrigger(
                i.name,
                i.schema,
                i.table_name,
                i.proc_schema,
                i.proc_name,
                i.enabled,
                i.full_definition,
            )
            for i in q
        ]
        self.triggers = {t.signature: t for t in triggers}

    def load_types(self):
        q = self.execute(self.TYPES_QUERY)

        def col(defn):
            return defn["attribute"], defn["type"]

        types = [
            InspectedType(i.name, i.schema, dict(col(_) for _ in i.columns)) for i in q
        ]
        self.types = {t.signature: t for t in types}

    def load_domains(self):
        q = self.execute(self.DOMAINS_QUERY)

        domains = [
            InspectedDomain(
                i.name,
                i.schema,
                i.data_type,
                i.collation,
                i.constraint_name,
                i.not_null,
                i.default,
                i.check,
            )
            for i in q
        ]
        self.domains = {t.signature: t for t in domains}

    def load_range_types(self):
        q = self.execute(self.RANGE_TYPES_QUERY)
        range_types = [
            InspectedRangeType(
                name=i.name,
                schema=i.schema,
                subtype=i.subtype,
                collation=i.collation,
                subtype_opclass=i.subtype_opclass,
                canonical=i.canonical,
                subtype_diff=i.subtype_diff,
            )
            for i in q
        ]
        self.range_types = {t.signature: t for t in range_types}

    def load_comments(self):
        q = self.execute(self.COMMENTS_QUERY)
        comments = [
            InspectedComment(
                object_type=i.object_type,
                schema=i.schema,
                name=i.name,
                column_name=i.column_name,
                comment=i.comment,
            )
            for i in q
        ]
        self.comments = {c.key: c for c in comments}

    def load_roles(self):
        q = self.execute(self.ROLES_QUERY)
        roles = [
            InspectedRole(
                name=i.name,
                superuser=i.superuser,
                inherit=i.inherit,
                createrole=i.createrole,
                createdb=i.createdb,
                login=i.login,
                replication=i.replication,
                bypassrls=i.bypassrls,
                connlimit=i.connlimit,
                member_of=i.member_of,
            )
            for i in q
        ]
        self.roles = {r.name: r for r in roles}

    def _filterable_props(self):
        return _FILTERABLE_PROPS

    def filter_schema(self, schema=None, exclude_schema=None):
        if schema and exclude_schema:
            raise ValueError("Can only have schema or exclude schema, not both")

        def equal_to_schema(x):
            return x.schema == schema

        def not_equal_to_exclude_schema(x):
            return x.schema != exclude_schema

        if schema:
            comparator = equal_to_schema
        elif exclude_schema:
            comparator = not_equal_to_exclude_schema
        else:
            raise ValueError("schema or exclude_schema must be not be none")

        for prop in self._filterable_props():
            att = getattr(self, prop)
            filtered = {k: v for k, v in att.items() if comparator(v)}
            setattr(self, prop, filtered)

    def _as_dicts(self):
        done = set()

        def obj_to_d(x, k=None):
            if id(x) in done:
                if isinstance(x, (str, bool, int)):
                    return x
                elif hasattr(x, "quoted_full_name"):
                    return x.quoted_full_name
                return "..."
            done.add(id(x))

            if isinstance(x, dict):
                return {k: obj_to_d(v, k) for k, v in x.items()}

            elif isinstance(x, (ColumnInfo, Inspected)):

                def safe_getattr(x, k):
                    try:
                        return getattr(x, k)
                    except NotImplementedError:
                        return "NOT IMPLEMENTED"

                return {
                    k: obj_to_d(safe_getattr(x, k), k)
                    for k in dir(x)
                    if not k.startswith("_") and not callable(safe_getattr(x, k))
                }
            else:
                return str(x)

        d = {}

        for prop in self._filterable_props():
            att = getattr(self, prop)

            _d = {k: obj_to_d(v) for k, v in att.items()}

            d[prop] = _d

        return d

    def encodeable_definition(self):
        return self._as_dicts()

    def as_yaml(self):
        s = StringIO()
        yaml.safe_dump(self.encodeable_definition(), s)
        return s.getvalue()

    def one_schema(self, schema):
        self.filter_schema(schema=schema)

    def filter_schemas(self, schemas):
        """Filter to only include objects from the specified schemas."""

        def in_schemas(x):
            return x.schema in schemas

        for prop in self._filterable_props():
            att = getattr(self, prop)
            filtered = {k: v for k, v in att.items() if in_schemas(v)}
            setattr(self, prop, filtered)

    def exclude_schema(self, schema):
        self.filter_schema(exclude_schema=schema)

    def __eq__(self, other):
        if type(self) != type(other):
            return False

        for obj_type in REGISTRY.values():
            if obj_type.include_in_eq:
                if getattr(self, obj_type.name) != getattr(other, obj_type.name):
                    return False

        if self.relations != other.relations:
            return False
        if self.functions != other.functions:
            return False

        return True
