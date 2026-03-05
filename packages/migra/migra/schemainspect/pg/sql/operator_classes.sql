select
  oc.opcname as name,
  n.nspname as schema,
  am.amname as access_method,
  oc.opcdefault as is_default,
  pg_catalog.format_type(oc.opcintype, null) as type_name,
  f.opfname as family_name,
  fn.nspname as family_schema,
  pg_catalog.format_type(oc.opckeytype, null) as storage_type
from pg_opclass oc
join pg_namespace n on n.oid = oc.opcnamespace
join pg_am am on am.oid = oc.opcmethod
join pg_opfamily f on f.oid = oc.opcfamily
join pg_namespace fn on fn.oid = f.opfnamespace
-- SKIP_INTERNAL where n.nspname not in ('pg_catalog', 'information_schema')
  and not exists (
    select 1 from pg_depend d
    where d.classid = 'pg_opclass'::regclass
      and d.objid = oc.oid
      and d.deptype = 'e'
      and d.refclassid = 'pg_extension'::regclass
  )
order by n.nspname, oc.opcname, am.amname
