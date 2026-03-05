select
  d.dictname as name,
  n.nspname as schema,
  t.tmplname as template_name,
  tn.nspname as template_schema,
  d.dictinitoption as options
from pg_ts_dict d
join pg_namespace n on n.oid = d.dictnamespace
join pg_ts_template t on t.oid = d.dicttemplate
join pg_namespace tn on tn.oid = t.tmplnamespace
-- SKIP_INTERNAL where n.nspname not in ('pg_catalog', 'information_schema')
order by n.nspname, d.dictname
