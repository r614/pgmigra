select
  f.opfname as name,
  n.nspname as schema,
  am.amname as access_method
from pg_opfamily f
join pg_namespace n on n.oid = f.opfnamespace
join pg_am am on am.oid = f.opfmethod
-- SKIP_INTERNAL where n.nspname not in ('pg_catalog', 'information_schema')
  and not exists (
    select 1 from pg_depend d
    where d.classid = 'pg_opfamily'::regclass
      and d.objid = f.oid
      and d.deptype = 'e'
      and d.refclassid = 'pg_extension'::regclass
  )
order by n.nspname, f.opfname, am.amname
