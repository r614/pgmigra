select
  s.stxname as name,
  n.nspname as schema,
  sn.nspname as table_schema,
  c.relname as table_name,
  s.stxstattarget as stattarget,
  pg_get_statisticsobjdef(s.oid) as definition
from pg_statistic_ext s
join pg_namespace n on n.oid = s.stxnamespace
join pg_class c on c.oid = s.stxrelid
join pg_namespace sn on sn.oid = c.relnamespace
-- SKIP_INTERNAL where n.nspname not in ('pg_catalog', 'information_schema', 'pg_toast')
-- SKIP_INTERNAL and n.nspname not like 'pg_temp_%' and n.nspname not like 'pg_toast_temp_%'
order by n.nspname, s.stxname
