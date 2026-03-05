select
  o.oprname as name,
  n.nspname as schema,
  pg_catalog.format_type(o.oprleft, null) as left_type,
  pg_catalog.format_type(o.oprright, null) as right_type,
  pg_catalog.format_type(o.oprresult, null) as result_type,
  p.proname as function_name,
  pn.nspname as function_schema,
  pg_catalog.pg_get_function_identity_arguments(o.oprcode) as function_args,
  co.oprname as commutator_name,
  cn.nspname as commutator_schema,
  no.oprname as negator_name,
  nn.nspname as negator_schema,
  o.oprcanhash as can_hash,
  o.oprcanmerge as can_merge
from pg_operator o
join pg_namespace n on n.oid = o.oprnamespace
join pg_proc p on p.oid = o.oprcode
join pg_namespace pn on pn.oid = p.pronamespace
left join pg_operator co on co.oid = o.oprcom
left join pg_namespace cn on cn.oid = co.oprnamespace
left join pg_operator no on no.oid = o.oprnegate
left join pg_namespace nn on nn.oid = no.oprnamespace
-- SKIP_INTERNAL where n.nspname not in ('pg_catalog', 'information_schema')
  and not exists (
    select 1 from pg_depend d
    where d.classid = 'pg_operator'::regclass
      and d.objid = o.oid
      and d.deptype = 'e'
      and d.refclassid = 'pg_extension'::regclass
  )
order by n.nspname, o.oprname, left_type, right_type
