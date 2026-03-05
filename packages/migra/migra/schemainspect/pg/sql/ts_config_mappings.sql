select
  c.cfgname as config_name,
  cn.nspname as config_schema,
  tt.alias as token_type,
  d.dictname as dict_name,
  dn.nspname as dict_schema,
  m.mapseqno as seq_no
from pg_ts_config_map m
join pg_ts_config c on c.oid = m.mapcfg
join pg_namespace cn on cn.oid = c.cfgnamespace
join pg_ts_dict d on d.oid = m.mapdict
join pg_namespace dn on dn.oid = d.dictnamespace
join lateral ts_token_type(c.cfgparser) tt on tt.tokid = m.maptokentype
-- SKIP_INTERNAL where cn.nspname not in ('pg_catalog', 'information_schema')
order by cn.nspname, c.cfgname, tt.alias, m.mapseqno
