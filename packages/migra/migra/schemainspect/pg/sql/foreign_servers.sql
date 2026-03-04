select
  s.srvname as name,
  f.fdwname as fdw_name,
  pg_get_userbyid(s.srvowner) as owner,
  s.srvtype as server_type,
  s.srvversion as server_version,
  array_to_string(s.srvoptions, ', ') as options
from pg_foreign_server s
join pg_foreign_data_wrapper f on f.oid = s.srvfdw
order by s.srvname
