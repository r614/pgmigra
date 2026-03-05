select
  table_schema as schema,
  table_name as name,
  'table' as object_type,
  grantee as "user",
  privilege_type as privilege,
  null::text[] as columns
from information_schema.role_table_grants
where grantee != (
    select tableowner
    from pg_tables
    where schemaname = table_schema
    and tablename = table_name
)
-- SKIP_INTERNAL and table_schema not in ('pg_internal', 'pg_catalog', 'information_schema', 'pg_toast')
-- SKIP_INTERNAL and table_schema not like 'pg_temp_%' and table_schema not like 'pg_toast_temp_%'

union all

select
  column_grants.table_schema as schema,
  column_grants.table_name as name,
  'column' as object_type,
  column_grants.grantee as "user",
  column_grants.privilege_type as privilege,
  array_agg(column_grants.column_name order by column_grants.column_name) as columns
from information_schema.role_column_grants column_grants
where column_grants.grantee != (
    select tableowner
    from pg_tables
    where schemaname = column_grants.table_schema
    and tablename = column_grants.table_name
)
and not exists (
    select 1
    from information_schema.role_table_grants tg
    where tg.table_schema = column_grants.table_schema
    and tg.table_name = column_grants.table_name
    and tg.grantee = column_grants.grantee
    and tg.privilege_type = column_grants.privilege_type
)
-- SKIP_INTERNAL and column_grants.table_schema not in ('pg_internal', 'pg_catalog', 'information_schema', 'pg_toast')
-- SKIP_INTERNAL and column_grants.table_schema not like 'pg_temp_%' and column_grants.table_schema not like 'pg_toast_temp_%'
group by column_grants.table_schema, column_grants.table_name, column_grants.grantee, column_grants.privilege_type

order by schema, name, "user";
