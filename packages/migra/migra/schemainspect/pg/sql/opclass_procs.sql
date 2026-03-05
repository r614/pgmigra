select
  oc.opcname as class_name,
  n.nspname as class_schema,
  am.amname as access_method,
  ap.amprocnum as support_number,
  p.proname as function_name,
  pn.nspname as function_schema,
  pg_catalog.pg_get_function_identity_arguments(p.oid) as function_args
from pg_amproc ap
join pg_opclass oc on oc.opcfamily = ap.amprocfamily
join pg_namespace n on n.oid = oc.opcnamespace
join pg_am am on am.oid = oc.opcmethod
join pg_proc p on p.oid = ap.amproc
join pg_namespace pn on pn.oid = p.pronamespace
-- SKIP_INTERNAL where n.nspname not in ('pg_catalog', 'information_schema')
  and not exists (
    select 1 from pg_depend d
    where d.classid = 'pg_opclass'::regclass
      and d.objid = oc.oid
      and d.deptype = 'e'
      and d.refclassid = 'pg_extension'::regclass
  )
order by n.nspname, oc.opcname, am.amname, ap.amprocnum
