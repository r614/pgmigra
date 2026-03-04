select
  collname as name,
  n.nspname as schema,
  case collprovider
    when 'd' then 'database default'
    when 'i' then 'icu'
    when 'c' then 'libc'
  end
  as provider,
  collencoding as encoding,
  -- PRE_15 collcollate as lc_collate,
  -- 15_TO_16 coalesce(collcollate, colliculocale) as lc_collate,
  -- 17_AND_LATER coalesce(collcollate, colllocale) as lc_collate,
  collctype as lc_ctype,
  collversion as version
from
pg_collation c
INNER JOIN pg_namespace n
    ON n.oid=c.collnamespace
    -- SKIP_INTERNAL where nspname not in ('pg_internal', 'pg_catalog', 'information_schema', 'pg_toast')
    -- SKIP_INTERNAL and nspname not like 'pg_temp_%' and nspname not like 'pg_toast_temp_%'
order by 2, 1
