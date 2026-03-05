select
  p.pubname as name,
  p.puballtables as publish_all_tables,
  p.pubinsert as publish_insert,
  p.pubupdate as publish_update,
  p.pubdelete as publish_delete,
  p.pubtruncate as publish_truncate,
  p.pubviaroot as publish_via_partition_root,
  pg_get_userbyid(p.pubowner) as owner,
  coalesce(
    (select array_agg(quote_ident(n.nspname) || '.' || quote_ident(c.relname) order by n.nspname, c.relname)
     from pg_publication_rel pr
     join pg_class c on c.oid = pr.prrelid
     join pg_namespace n on n.oid = c.relnamespace
     where pr.prpubid = p.oid),
    '{}'
  ) as tables
from pg_publication p
order by p.pubname
