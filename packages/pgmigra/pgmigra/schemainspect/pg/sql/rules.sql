select
  r.rulename as name,
  n.nspname as schema,
  c.relname as table_name,
  r.ev_enabled as enabled,
  pg_get_ruledef(r.oid, true) as definition
from pg_rewrite r
join pg_class c on c.oid = r.ev_class
join pg_namespace n on n.oid = c.relnamespace
where r.rulename != '_RETURN'
-- SKIP_INTERNAL and n.nspname not in ('pg_catalog', 'information_schema', 'pg_toast')
-- SKIP_INTERNAL and n.nspname not like 'pg_temp_%' and n.nspname not like 'pg_toast_temp_%'
order by n.nspname, c.relname, r.rulename
