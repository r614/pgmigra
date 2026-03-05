select
  c.cfgname as name,
  n.nspname as schema,
  p.prsname as parser_name,
  pn.nspname as parser_schema
from pg_ts_config c
join pg_namespace n on n.oid = c.cfgnamespace
join pg_ts_parser p on p.oid = c.cfgparser
join pg_namespace pn on pn.oid = p.prsnamespace
-- SKIP_INTERNAL where n.nspname not in ('pg_catalog', 'information_schema')
order by n.nspname, c.cfgname
