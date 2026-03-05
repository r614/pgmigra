select
  pg_catalog.format_type(c.castsource, null) as source_type,
  pg_catalog.format_type(c.casttarget, null) as target_type,
  c.castcontext as context,
  c.castmethod as method,
  p.proname as function_name,
  pn.nspname as function_schema,
  pg_catalog.pg_get_function_identity_arguments(c.castfunc) as function_args
from pg_cast c
left join pg_proc p on p.oid = c.castfunc
left join pg_namespace pn on pn.oid = p.pronamespace
where c.oid >= 16384
  and not exists (
    select 1 from pg_depend d
    where d.classid = 'pg_cast'::regclass
      and d.objid = c.oid
      and d.deptype in ('i', 'e')
  )
order by source_type, target_type
