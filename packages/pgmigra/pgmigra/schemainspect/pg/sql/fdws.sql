select
  f.fdwname as name,
  pg_get_userbyid(f.fdwowner) as owner,
  h.proname as handler_name,
  hn.nspname as handler_schema,
  v.proname as validator_name,
  vn.nspname as validator_schema,
  array_to_string(f.fdwoptions, ', ') as options
from pg_foreign_data_wrapper f
left join pg_proc h on h.oid = f.fdwhandler
left join pg_namespace hn on hn.oid = h.pronamespace
left join pg_proc v on v.oid = f.fdwvalidator
left join pg_namespace vn on vn.oid = v.pronamespace
order by f.fdwname
