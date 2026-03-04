select
  um.srvname as server_name,
  um.usename as user_name,
  um.umoptions as options
from pg_user_mappings um
order by um.srvname, um.usename
