select
    r.rolname as name,
    r.rolsuper as superuser,
    r.rolinherit as inherit,
    r.rolcreaterole as createrole,
    r.rolcreatedb as createdb,
    r.rolcanlogin as login,
    r.rolreplication as replication,
    r.rolbypassrls as bypassrls,
    r.rolconnlimit as connlimit,
    array_agg(m.rolname order by m.rolname) filter (where m.rolname is not null) as member_of
from
    pg_roles r
    left join pg_auth_members am on am.member = r.oid
    left join pg_roles m on m.oid = am.roleid
where
    r.rolname not like 'pg_%'
    and r.rolname != 'postgres'
group by
    r.oid, r.rolname, r.rolsuper, r.rolinherit, r.rolcreaterole,
    r.rolcreatedb, r.rolcanlogin, r.rolreplication, r.rolbypassrls, r.rolconnlimit
order by
    r.rolname
