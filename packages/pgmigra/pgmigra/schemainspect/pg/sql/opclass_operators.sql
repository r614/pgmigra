select
  oc.opcname as class_name,
  n.nspname as class_schema,
  am.amname as access_method,
  ao.amopstrategy as strategy,
  o.oprname as operator_name,
  on2.nspname as operator_schema,
  pg_catalog.format_type(o.oprleft, null) as left_type,
  pg_catalog.format_type(o.oprright, null) as right_type
from pg_amop ao
join pg_opclass oc on oc.opcfamily = ao.amopfamily
  and oc.opcmethod = ao.amopmethod
join pg_namespace n on n.oid = oc.opcnamespace
join pg_am am on am.oid = oc.opcmethod
join pg_operator o on o.oid = ao.amopopr
join pg_namespace on2 on on2.oid = o.oprnamespace
-- SKIP_INTERNAL where n.nspname not in ('pg_catalog', 'information_schema')
  and not exists (
    select 1 from pg_depend d
    where d.classid = 'pg_opclass'::regclass
      and d.objid = oc.oid
      and d.deptype = 'e'
      and d.refclassid = 'pg_extension'::regclass
  )
order by n.nspname, oc.opcname, am.amname, ao.amopstrategy
