select
    d.objoid,
    d.objsubid,
    d.description as comment,
    case
        when c.relkind in ('r', 'p') then 'table'
        when c.relkind = 'v' then 'view'
        when c.relkind = 'm' then 'materialized view'
        else 'table'
    end as object_type,
    n.nspname as schema,
    c.relname as name,
    case when d.objsubid > 0 then a.attname else null end as column_name
from
    pg_description d
    join pg_class c on c.oid = d.objoid
    join pg_namespace n on n.oid = c.relnamespace
    left join pg_attribute a on a.attrelid = d.objoid and a.attnum = d.objsubid
where
    d.classoid = 'pg_class'::regclass
    and c.relkind in ('r', 'v', 'm', 'p')
    -- SKIP_INTERNAL and n.nspname not in ('pg_catalog', 'information_schema', 'pg_toast')
    -- SKIP_INTERNAL and n.nspname not like 'pg_temp_%' and n.nspname not like 'pg_toast_temp_%'

union all

select
    d.objoid,
    d.objsubid,
    d.description as comment,
    'function' as object_type,
    n.nspname as schema,
    p.proname as name,
    null as column_name
from
    pg_description d
    join pg_proc p on p.oid = d.objoid
    join pg_namespace n on n.oid = p.pronamespace
where
    d.classoid = 'pg_proc'::regclass
    -- SKIP_INTERNAL and n.nspname not in ('pg_catalog', 'information_schema', 'pg_toast')
    -- SKIP_INTERNAL and n.nspname not like 'pg_temp_%' and n.nspname not like 'pg_toast_temp_%'

order by object_type, schema, name, column_name
