with extension_oids as (
  select
      objid
  from
      pg_depend d
  WHERE
      d.refclassid = 'pg_extension'::regclass and
      d.classid = 'pg_type'::regclass
)
SELECT
  n.nspname as "schema",
  t.typname as "name",
  format_type(st.oid, NULL) as "subtype",
  CASE WHEN r.rngcollation <> 0 AND r.rngcollation <> st.typcollation
       THEN (SELECT c.collname FROM pg_collation c WHERE c.oid = r.rngcollation)
       ELSE NULL
  END as "collation",
  CASE WHEN opc.opcdefault THEN NULL
       ELSE opc.opcname
  END as "subtype_opclass",
  CASE WHEN r.rngcanonical = 0 THEN NULL
       ELSE r.rngcanonical::regproc::text
  END as "canonical",
  CASE WHEN r.rngsubdiff = 0 THEN NULL
       ELSE r.rngsubdiff::regproc::text
  END as "subtype_diff"
FROM pg_range r
JOIN pg_type t ON t.oid = r.rngtypid
JOIN pg_namespace n ON n.oid = t.typnamespace
JOIN pg_type st ON st.oid = r.rngsubtype
JOIN pg_opclass opc ON opc.oid = r.rngsubopc
LEFT JOIN extension_oids e ON t.oid = e.objid
WHERE
  e.objid IS NULL
  -- SKIP_INTERNAL and n.nspname not in ('pg_internal', 'pg_catalog', 'information_schema', 'pg_toast')
  -- SKIP_INTERNAL and n.nspname not like 'pg_temp_%' and n.nspname not like 'pg_toast_temp_%'
ORDER BY 1, 2;
