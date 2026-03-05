select
  e.evtname as name,
  pg_get_userbyid(e.evtowner) as owner,
  e.evtevent as event,
  e.evtenabled as enabled,
  array_to_string(e.evttags, ', ') as tags,
  p.proname as function_name,
  n.nspname as function_schema
from pg_event_trigger e
join pg_proc p on p.oid = e.evtfoid
join pg_namespace n on n.oid = p.pronamespace
order by e.evtname
