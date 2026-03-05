"""
Comprehensive integration test exercising all DDL object types in a realistic schema.

Tests that migra can diff two databases with a rich, realistic schema and produce
a migration that makes them identical (idempotency check).
"""

from pgmigra import Migration
from pgmigra.changes import Changes
from pgmigra.db import connect, temporary_database
from pgmigra.schemainspect import get_inspector

# ---------------------------------------------------------------------------
# Schema A: the "source" (current production-like database)
# ---------------------------------------------------------------------------
SCHEMA_A = """
-- === Schemas ===
CREATE SCHEMA inventory;
CREATE SCHEMA analytics;

-- === Enums ===
CREATE TYPE public.order_status AS ENUM ('pending', 'processing', 'shipped', 'delivered');
CREATE TYPE inventory.condition_type AS ENUM ('new', 'refurbished', 'used');

-- === Domains ===
CREATE DOMAIN public.email_address AS text
  CHECK (VALUE ~ '^[^@]+@[^@]+\\.[^@]+$');
CREATE DOMAIN public.positive_int AS integer
  CHECK (VALUE > 0);

-- === Range Types ===
CREATE TYPE public.price_range AS RANGE (subtype = numeric);

-- === Sequences ===
CREATE SEQUENCE public.global_id_seq START 1000;
CREATE SEQUENCE inventory.stock_seq START 1 INCREMENT 10;

-- === Tables ===
CREATE TABLE public.customers (
    id serial PRIMARY KEY,
    name text NOT NULL,
    email public.email_address NOT NULL UNIQUE,
    status public.order_status DEFAULT 'pending',
    created_at timestamptz DEFAULT now()
);

CREATE TABLE public.products (
    id serial PRIMARY KEY,
    sku text NOT NULL UNIQUE,
    name text NOT NULL,
    price numeric(10,2) NOT NULL CHECK (price >= 0),
    created_at timestamptz DEFAULT now()
);

CREATE TABLE public.orders (
    id serial PRIMARY KEY,
    customer_id integer NOT NULL REFERENCES public.customers(id),
    status public.order_status DEFAULT 'pending',
    total numeric(10,2),
    ordered_at timestamptz DEFAULT now()
);

CREATE TABLE public.order_items (
    id serial PRIMARY KEY,
    order_id integer NOT NULL REFERENCES public.orders(id),
    product_id integer NOT NULL REFERENCES public.products(id),
    quantity public.positive_int NOT NULL,
    unit_price numeric(10,2) NOT NULL
);

CREATE TABLE inventory.warehouse (
    id serial PRIMARY KEY,
    name text NOT NULL,
    location text
);

CREATE TABLE inventory.stock (
    warehouse_id integer NOT NULL REFERENCES inventory.warehouse(id),
    product_id integer NOT NULL REFERENCES public.products(id),
    quantity integer NOT NULL DEFAULT 0,
    condition inventory.condition_type DEFAULT 'new',
    PRIMARY KEY (warehouse_id, product_id)
);

-- === Views ===
CREATE VIEW public.active_orders AS
  SELECT o.id, o.customer_id, c.name AS customer_name, o.status, o.total
  FROM public.orders o
  JOIN public.customers c ON c.id = o.customer_id
  WHERE o.status IN ('pending', 'processing');

-- === Materialized Views ===
CREATE MATERIALIZED VIEW analytics.order_summary AS
  SELECT
    date_trunc('month', o.ordered_at) AS month,
    count(*) AS order_count,
    sum(o.total) AS revenue
  FROM public.orders o
  GROUP BY 1;

-- === Indexes ===
CREATE INDEX idx_orders_customer ON public.orders (customer_id);
CREATE INDEX idx_orders_status ON public.orders (status);
CREATE INDEX idx_order_items_product ON public.order_items (product_id);
CREATE INDEX idx_stock_product ON inventory.stock (product_id);
CREATE UNIQUE INDEX idx_order_summary_month ON analytics.order_summary (month);

-- === Functions ===
CREATE FUNCTION public.calculate_order_total(p_order_id integer)
RETURNS numeric AS $$
  SELECT COALESCE(SUM(quantity * unit_price), 0)
  FROM public.order_items
  WHERE order_id = p_order_id;
$$ LANGUAGE sql STABLE;

CREATE FUNCTION public.update_order_total()
RETURNS trigger AS $$
BEGIN
  UPDATE public.orders
  SET total = public.calculate_order_total(NEW.order_id)
  WHERE id = NEW.order_id;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION public.audit_log_func()
RETURNS trigger AS $$
BEGIN
  RAISE NOTICE 'audit: % on %', TG_OP, TG_TABLE_NAME;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION public.ddl_command_logger()
RETURNS event_trigger AS $$
BEGIN
  RAISE NOTICE 'DDL command: %', tg_tag;
END;
$$ LANGUAGE plpgsql;

-- === Triggers ===
CREATE TRIGGER trg_update_order_total
  AFTER INSERT OR UPDATE ON public.order_items
  FOR EACH ROW EXECUTE FUNCTION public.update_order_total();

CREATE TRIGGER trg_audit_customers
  AFTER INSERT OR UPDATE OR DELETE ON public.customers
  FOR EACH ROW EXECUTE FUNCTION public.audit_log_func();

-- === Rules ===
CREATE RULE protect_shipped_orders AS
  ON DELETE TO public.orders
  WHERE OLD.status = 'shipped'
  DO INSTEAD NOTHING;

-- === Statistics ===
CREATE STATISTICS public.stats_order_items_product_qty (dependencies)
  ON product_id, quantity FROM public.order_items;

-- === Comments ===
COMMENT ON TABLE public.customers IS 'Main customer table';
COMMENT ON COLUMN public.customers.email IS 'Must be a valid email address';
COMMENT ON TABLE public.products IS 'Product catalog';

-- === RLS Policies ===
ALTER TABLE public.orders ENABLE ROW LEVEL SECURITY;
CREATE POLICY orders_owner_policy ON public.orders
  USING (customer_id = current_setting('app.current_customer_id')::integer);

-- === Collations ===
CREATE COLLATION public.case_insensitive (provider = icu, locale = 'und-u-ks-level2', deterministic = false);

-- === Text Search Dictionaries ===
CREATE TEXT SEARCH DICTIONARY public.english_simple (
  TEMPLATE = pg_catalog.simple,
  StopWords = english
);

-- === Text Search Configurations ===
CREATE TEXT SEARCH CONFIGURATION public.english_custom (PARSER = pg_catalog.default);
ALTER TEXT SEARCH CONFIGURATION public.english_custom
  ADD MAPPING FOR asciiword WITH public.english_simple;
ALTER TEXT SEARCH CONFIGURATION public.english_custom
  ADD MAPPING FOR word WITH public.english_simple;

-- === Event Triggers ===
CREATE EVENT TRIGGER evt_ddl_logger ON ddl_command_end
  EXECUTE FUNCTION public.ddl_command_logger();

-- === Casts ===
CREATE FUNCTION public.positive_int_to_text(public.positive_int)
RETURNS text AS $$
  SELECT $1::integer::text;
$$ LANGUAGE sql IMMUTABLE;

CREATE CAST (public.positive_int AS text)
  WITH FUNCTION public.positive_int_to_text(public.positive_int)
  AS ASSIGNMENT;

-- === Operators ===
CREATE FUNCTION public.price_overlap(public.price_range, public.price_range)
RETURNS boolean AS $$
  SELECT $1 && $2;
$$ LANGUAGE sql IMMUTABLE;

CREATE OPERATOR public.<<>> (
  FUNCTION = public.price_overlap,
  LEFTARG = public.price_range,
  RIGHTARG = public.price_range,
  COMMUTATOR = OPERATOR(public.<<>>)
);

-- === Operator Families ===
CREATE OPERATOR FAMILY public.custom_int_ops USING btree;

-- === Operator Classes ===
CREATE FUNCTION public.custom_int_cmp(integer, integer) RETURNS integer AS $$
  SELECT CASE WHEN $1 < $2 THEN -1 WHEN $1 > $2 THEN 1 ELSE 0 END;
$$ LANGUAGE sql IMMUTABLE;

CREATE OPERATOR CLASS public.custom_int_btree_ops
  FOR TYPE integer USING btree
  FAMILY public.custom_int_ops AS
  OPERATOR 1 <,
  OPERATOR 2 <=,
  OPERATOR 3 =,
  OPERATOR 4 >=,
  OPERATOR 5 >,
  FUNCTION 1 public.custom_int_cmp(integer, integer);

-- === Publications ===
CREATE PUBLICATION pub_orders FOR TABLE public.orders, public.order_items;
""".strip()


# ---------------------------------------------------------------------------
# Schema B: the "target" (desired new state with modifications)
# ---------------------------------------------------------------------------
SCHEMA_B = """
-- === Schemas ===
CREATE SCHEMA inventory;
CREATE SCHEMA analytics;
CREATE SCHEMA reporting;   -- NEW schema

-- === Enums ===
CREATE TYPE public.order_status AS ENUM ('pending', 'processing', 'shipped', 'delivered', 'cancelled');  -- added 'cancelled'
CREATE TYPE inventory.condition_type AS ENUM ('new', 'refurbished', 'used');
CREATE TYPE reporting.report_format AS ENUM ('pdf', 'csv', 'json');  -- NEW enum

-- === Domains ===
CREATE DOMAIN public.email_address AS text
  CHECK (VALUE ~ '^[^@]+@[^@]+\\.[^@]+$');
CREATE DOMAIN public.positive_int AS integer
  CHECK (VALUE > 0);
CREATE DOMAIN public.percentage AS numeric(5,2)
  CHECK (VALUE >= 0 AND VALUE <= 100);  -- NEW domain

-- === Range Types ===
CREATE TYPE public.price_range AS RANGE (subtype = numeric);

-- === Sequences ===
CREATE SEQUENCE public.global_id_seq START 1000;
-- inventory.stock_seq REMOVED
CREATE SEQUENCE reporting.report_seq START 1;  -- NEW sequence

-- === Tables ===
CREATE TABLE public.customers (
    id serial PRIMARY KEY,
    name text NOT NULL,
    email public.email_address NOT NULL UNIQUE,
    status public.order_status DEFAULT 'pending',
    phone text,  -- NEW column
    created_at timestamptz DEFAULT now()
);

CREATE TABLE public.products (
    id serial PRIMARY KEY,
    sku text NOT NULL UNIQUE,
    name text NOT NULL,
    price numeric(10,2) NOT NULL CHECK (price >= 0),
    discount public.percentage,  -- NEW column using new domain
    created_at timestamptz DEFAULT now()
);

CREATE TABLE public.orders (
    id serial PRIMARY KEY,
    customer_id integer NOT NULL REFERENCES public.customers(id),
    status public.order_status DEFAULT 'pending',
    total numeric(10,2),
    notes text,  -- NEW column
    ordered_at timestamptz DEFAULT now()
);

CREATE TABLE public.order_items (
    id serial PRIMARY KEY,
    order_id integer NOT NULL REFERENCES public.orders(id),
    product_id integer NOT NULL REFERENCES public.products(id),
    quantity public.positive_int NOT NULL,
    unit_price numeric(10,2) NOT NULL
);

CREATE TABLE inventory.warehouse (
    id serial PRIMARY KEY,
    name text NOT NULL,
    location text,
    capacity integer  -- NEW column
);

CREATE TABLE inventory.stock (
    warehouse_id integer NOT NULL REFERENCES inventory.warehouse(id),
    product_id integer NOT NULL REFERENCES public.products(id),
    quantity integer NOT NULL DEFAULT 0,
    condition inventory.condition_type DEFAULT 'new',
    PRIMARY KEY (warehouse_id, product_id)
);

-- NEW table
CREATE TABLE reporting.daily_reports (
    id serial PRIMARY KEY,
    report_date date NOT NULL,
    format reporting.report_format NOT NULL DEFAULT 'pdf',
    generated_at timestamptz DEFAULT now()
);

-- === Views ===
-- MODIFIED: added phone column
CREATE VIEW public.active_orders AS
  SELECT o.id, o.customer_id, c.name AS customer_name, c.phone, o.status, o.total
  FROM public.orders o
  JOIN public.customers c ON c.id = o.customer_id
  WHERE o.status IN ('pending', 'processing');

-- NEW view
CREATE VIEW reporting.revenue_by_month AS
  SELECT
    date_trunc('month', o.ordered_at) AS month,
    sum(o.total) AS revenue
  FROM public.orders o
  GROUP BY 1;

-- === Materialized Views ===
-- MODIFIED: added avg_total
CREATE MATERIALIZED VIEW analytics.order_summary AS
  SELECT
    date_trunc('month', o.ordered_at) AS month,
    count(*) AS order_count,
    sum(o.total) AS revenue,
    avg(o.total) AS avg_total
  FROM public.orders o
  GROUP BY 1;

-- === Indexes ===
CREATE INDEX idx_orders_customer ON public.orders (customer_id);
-- idx_orders_status REMOVED
CREATE INDEX idx_orders_ordered_at ON public.orders (ordered_at);  -- NEW index
CREATE INDEX idx_order_items_product ON public.order_items (product_id);
CREATE INDEX idx_stock_product ON inventory.stock (product_id);
CREATE UNIQUE INDEX idx_order_summary_month ON analytics.order_summary (month);

-- === Functions ===
CREATE FUNCTION public.calculate_order_total(p_order_id integer)
RETURNS numeric AS $$
  SELECT COALESCE(SUM(quantity * unit_price), 0)
  FROM public.order_items
  WHERE order_id = p_order_id;
$$ LANGUAGE sql STABLE;

CREATE FUNCTION public.update_order_total()
RETURNS trigger AS $$
BEGIN
  UPDATE public.orders
  SET total = public.calculate_order_total(NEW.order_id)
  WHERE id = NEW.order_id;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION public.audit_log_func()
RETURNS trigger AS $$
BEGIN
  RAISE NOTICE 'audit v2: % on %', TG_OP, TG_TABLE_NAME;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION public.ddl_command_logger()
RETURNS event_trigger AS $$
BEGIN
  RAISE NOTICE 'DDL command: %', tg_tag;
END;
$$ LANGUAGE plpgsql;

-- NEW function for new event trigger
CREATE FUNCTION public.ddl_drop_logger()
RETURNS event_trigger AS $$
BEGIN
  RAISE NOTICE 'DDL drop: %', tg_tag;
END;
$$ LANGUAGE plpgsql;

-- === Triggers ===
CREATE TRIGGER trg_update_order_total
  AFTER INSERT OR UPDATE ON public.order_items
  FOR EACH ROW EXECUTE FUNCTION public.update_order_total();

-- trg_audit_customers REMOVED

-- === Rules ===
-- protect_shipped_orders REMOVED
-- NEW rule
CREATE RULE redirect_old_orders AS
  ON INSERT TO public.orders
  WHERE NEW.ordered_at < '2020-01-01'
  DO INSTEAD NOTHING;

-- === Statistics ===
CREATE STATISTICS public.stats_order_items_product_qty (dependencies)
  ON product_id, quantity FROM public.order_items;
-- NEW statistics
CREATE STATISTICS public.stats_orders_customer_status (dependencies)
  ON customer_id, status FROM public.orders;

-- === Comments ===
COMMENT ON TABLE public.customers IS 'Main customer table with contact info';  -- MODIFIED
-- products comment REMOVED
COMMENT ON COLUMN public.customers.email IS 'Must be a valid email address';
COMMENT ON TABLE reporting.daily_reports IS 'Auto-generated daily reports';  -- NEW

-- === RLS Policies ===
ALTER TABLE public.orders ENABLE ROW LEVEL SECURITY;
-- MODIFIED policy: added notes IS NOT NULL
CREATE POLICY orders_owner_policy ON public.orders
  USING (customer_id = current_setting('app.current_customer_id')::integer AND notes IS NOT NULL);

-- === Collations ===
CREATE COLLATION public.case_insensitive (provider = icu, locale = 'und-u-ks-level2', deterministic = false);
-- NEW collation
CREATE COLLATION public.numeric_sort (provider = icu, locale = 'en-u-kn-true', deterministic = false);

-- === Text Search Dictionaries ===
-- english_simple REMOVED
-- NEW dict
CREATE TEXT SEARCH DICTIONARY public.english_stem (
  TEMPLATE = pg_catalog.snowball,
  Language = english
);

-- === Text Search Configurations ===
-- english_custom REMOVED
-- NEW config
CREATE TEXT SEARCH CONFIGURATION public.english_fulltext (PARSER = pg_catalog.default);
ALTER TEXT SEARCH CONFIGURATION public.english_fulltext
  ADD MAPPING FOR asciiword WITH public.english_stem;
ALTER TEXT SEARCH CONFIGURATION public.english_fulltext
  ADD MAPPING FOR word WITH public.english_stem;
ALTER TEXT SEARCH CONFIGURATION public.english_fulltext
  ADD MAPPING FOR numword WITH simple;

-- === Event Triggers ===
-- evt_ddl_logger REMOVED
-- NEW event trigger
CREATE EVENT TRIGGER evt_drop_logger ON sql_drop
  EXECUTE FUNCTION public.ddl_drop_logger();

-- === Casts ===
CREATE FUNCTION public.positive_int_to_text(public.positive_int)
RETURNS text AS $$
  SELECT $1::integer::text;
$$ LANGUAGE sql IMMUTABLE;

CREATE CAST (public.positive_int AS text)
  WITH FUNCTION public.positive_int_to_text(public.positive_int)
  AS ASSIGNMENT;

-- === Operators ===
CREATE FUNCTION public.price_overlap(public.price_range, public.price_range)
RETURNS boolean AS $$
  SELECT $1 && $2;
$$ LANGUAGE sql IMMUTABLE;

CREATE OPERATOR public.<<>> (
  FUNCTION = public.price_overlap,
  LEFTARG = public.price_range,
  RIGHTARG = public.price_range,
  COMMUTATOR = OPERATOR(public.<<>>)
);

-- NEW operator
CREATE FUNCTION public.price_contains(public.price_range, numeric)
RETURNS boolean AS $$
  SELECT $1 @> $2;
$$ LANGUAGE sql IMMUTABLE;

CREATE OPERATOR public.@>@ (
  FUNCTION = public.price_contains,
  LEFTARG = public.price_range,
  RIGHTARG = numeric
);

-- === Operator Families ===
CREATE OPERATOR FAMILY public.custom_int_ops USING btree;
-- NEW family
CREATE OPERATOR FAMILY public.custom_text_ops USING hash;

-- === Operator Classes ===
CREATE FUNCTION public.custom_int_cmp(integer, integer) RETURNS integer AS $$
  SELECT CASE WHEN $1 < $2 THEN -1 WHEN $1 > $2 THEN 1 ELSE 0 END;
$$ LANGUAGE sql IMMUTABLE;

CREATE OPERATOR CLASS public.custom_int_btree_ops
  FOR TYPE integer USING btree
  FAMILY public.custom_int_ops AS
  OPERATOR 1 <,
  OPERATOR 2 <=,
  OPERATOR 3 =,
  OPERATOR 4 >=,
  OPERATOR 5 >,
  FUNCTION 1 public.custom_int_cmp(integer, integer);

-- === Publications ===
-- MODIFIED: added customers table
CREATE PUBLICATION pub_orders FOR TABLE public.orders, public.order_items, public.customers;
""".strip()


def test_comprehensive_migration_idempotency():
    """
    Create two databases with rich schemas covering all DDL object types,
    generate a migration from A→B, apply it, then verify no diff remains.
    """
    with temporary_database() as url_a, temporary_database() as url_b:
        with connect(url_a) as sa, connect(url_b) as sb:
            sa.execute(SCHEMA_A)
            sb.execute(SCHEMA_B)

        with connect(url_a) as sa, connect(url_b) as sb:
            m = Migration(sa, sb)
            m.set_safety(False)
            m.add_all_changes(privileges=True)

            sql = m.sql
            assert sql.strip(), "Expected non-empty migration SQL"

            # Verify key migration operations are present
            assert "reporting" in sql.lower()
            assert "cancelled" in sql.lower()  # enum addition

            m.apply()

            # Second pass: should produce zero diff (idempotency)
            m.add_all_changes(privileges=True)
            remaining = m.sql.strip()
            assert remaining == "", (
                f"Expected empty migration after apply, got:\n{remaining}"
            )


def test_comprehensive_changes_detail():
    """
    Verify that the Changes object detects specific additions, drops,
    and modifications across all object types.
    """
    with temporary_database() as url_a, temporary_database() as url_b:
        with connect(url_a) as sa, connect(url_b) as sb:
            sa.execute(SCHEMA_A)
            sb.execute(SCHEMA_B)
            i_from = get_inspector(sa)
            i_target = get_inspector(sb)

        changes = Changes(i_from, i_target)

        # --- Schemas ---
        stmts = changes.schemas(creations_only=True)
        assert any("reporting" in s for s in stmts)

        # --- Enums ---
        stmts = changes.enums(creations_only=True, modifications=False)
        sql = stmts.sql
        assert "report_format" in sql

        # --- Domains ---
        stmts = changes.domains(creations_only=True)
        assert any("percentage" in s for s in stmts)

        # --- Sequences ---
        seq_creates = changes.sequences(creations_only=True)
        assert any("report_seq" in s for s in seq_creates)
        seq_drops = changes.sequences(drops_only=True)
        seq_drops.safe = False
        assert any("stock_seq" in s for s in seq_drops)

        # --- Functions: audit_log_func should be altered ---
        nontable = changes.non_table_selectable_creations()
        # The view active_orders is modified, so it should be recreated
        sql = nontable.sql
        assert "active_orders" in sql or "revenue_by_month" in sql

        # --- Triggers ---
        trigger_drops = changes.triggers(drops_only=True)
        trigger_drops.safe = False
        assert any("trg_audit_customers" in s for s in trigger_drops)

        # --- Rules ---
        rule_creates = changes.rules(creations_only=True)
        assert any("redirect_old_orders" in s for s in rule_creates)
        rule_drops = changes.rules(drops_only=True)
        rule_drops.safe = False
        assert any("protect_shipped_orders" in s for s in rule_drops)

        # --- Statistics ---
        stat_creates = changes.statistics(creations_only=True)
        assert any("stats_orders_customer_status" in s for s in stat_creates)

        # --- Comments ---
        comment_creates = changes.comments(creations_only=True)
        sql = comment_creates.sql
        assert "daily_reports" in sql or "contact info" in sql

        # --- Collations ---
        collation_creates = changes.collations(creations_only=True)
        assert any("numeric_sort" in s for s in collation_creates)

        # --- Text Search Dicts ---
        ts_dict_creates = changes.ts_dicts(creations_only=True)
        assert any("english_stem" in s for s in ts_dict_creates)
        ts_dict_drops = changes.ts_dicts(drops_only=True)
        ts_dict_drops.safe = False
        assert any("english_simple" in s for s in ts_dict_drops)

        # --- Text Search Configs ---
        ts_config_creates = changes.ts_configs(creations_only=True)
        assert any("english_fulltext" in s for s in ts_config_creates)
        ts_config_drops = changes.ts_configs(drops_only=True)
        ts_config_drops.safe = False
        assert any("english_custom" in s for s in ts_config_drops)

        # --- Event Triggers ---
        evt_creates = changes.event_triggers(creations_only=True)
        assert any("evt_drop_logger" in s for s in evt_creates)
        evt_drops = changes.event_triggers(drops_only=True)
        evt_drops.safe = False
        assert any("evt_ddl_logger" in s for s in evt_drops)

        # --- Operators ---
        op_creates = changes.operators(creations_only=True)
        assert any("@>@" in s for s in op_creates)

        # --- Operator Families ---
        opf_creates = changes.operator_families(creations_only=True)
        assert any("custom_text_ops" in s for s in opf_creates)

        # --- Publications ---
        pub_stmts = changes.publications()
        sql = pub_stmts.sql
        assert "pub_orders" in sql

        # --- Casts (no changes expected since cast is identical) ---
        cast_creates = changes.casts(creations_only=True)
        cast_drops = changes.casts(drops_only=True)
        assert len(cast_creates) == 0
        assert len(cast_drops) == 0

        # --- RLS Policies ---
        rls_drops = changes.rlspolicies(drops_only=True)
        rls_drops.safe = False
        assert any("orders_owner_policy" in s for s in rls_drops)
        rls_creates = changes.rlspolicies(creations_only=True)
        assert any("orders_owner_policy" in s for s in rls_creates)


def test_comprehensive_reverse_migration():
    """
    Apply B→A migration (reverse direction) and verify idempotency.
    """
    with temporary_database() as url_a, temporary_database() as url_b:
        with connect(url_a) as sa, connect(url_b) as sb:
            sa.execute(SCHEMA_B)
            sb.execute(SCHEMA_A)

        with connect(url_a) as sa, connect(url_b) as sb:
            m = Migration(sa, sb)
            m.set_safety(False)
            m.add_all_changes(privileges=True)

            sql = m.sql
            assert sql.strip(), "Expected non-empty reverse migration SQL"

            m.apply()

            m.add_all_changes(privileges=True)
            remaining = m.sql.strip()
            assert remaining == "", (
                f"Expected empty migration after reverse apply, got:\n{remaining}"
            )


def test_comprehensive_from_empty():
    """
    Migrate from an empty database to the full Schema B and verify idempotency.
    """
    with temporary_database() as url_empty, temporary_database() as url_b:
        with connect(url_b) as sb:
            sb.execute(SCHEMA_B)

        with connect(url_empty) as se, connect(url_b) as sb:
            m = Migration(se, sb)
            m.set_safety(False)
            m.add_all_changes(privileges=True)

            sql = m.sql
            assert sql.strip()

            m.apply()

            m.add_all_changes(privileges=True)
            remaining = m.sql.strip()
            assert remaining == "", (
                f"Expected empty migration from empty→B, got:\n{remaining}"
            )


def test_comprehensive_to_empty():
    """
    Migrate from the full Schema A to an empty database (drop everything)
    and verify idempotency.
    """
    with temporary_database() as url_a, temporary_database() as url_empty:
        with connect(url_a) as sa:
            sa.execute(SCHEMA_A)

        with connect(url_a) as sa, connect(url_empty) as se:
            m = Migration(sa, se)
            m.set_safety(False)
            m.add_all_changes(privileges=True)

            sql = m.sql
            assert sql.strip()

            m.apply()

            m.add_all_changes(privileges=True)
            remaining = m.sql.strip()
            assert remaining == "", (
                f"Expected empty migration from A→empty, got:\n{remaining}"
            )


def test_comprehensive_inspect_roundtrip():
    """
    Inspect Schema A, verify key objects are present across all categories.
    """
    with temporary_database() as url:
        with connect(url) as s:
            s.execute(SCHEMA_A)
            i = get_inspector(s)

            # Schemas (schemas dict uses unquoted keys)
            assert "inventory" in i.schemas
            assert "analytics" in i.schemas

            # Enums
            assert '"public"."order_status"' in i.enums

            # Domains
            assert '"public"."email_address"' in i.domains
            assert '"public"."positive_int"' in i.domains

            # Range types
            assert '"public"."price_range"' in i.range_types

            # Sequences
            assert '"public"."global_id_seq"' in i.sequences

            # Tables
            assert '"public"."customers"' in i.tables
            assert '"public"."products"' in i.tables
            assert '"public"."orders"' in i.tables
            assert '"public"."order_items"' in i.tables
            assert '"inventory"."warehouse"' in i.tables
            assert '"inventory"."stock"' in i.tables

            # Views
            assert '"public"."active_orders"' in i.views

            # Materialized views
            assert '"analytics"."order_summary"' in i.materialized_views

            # Indexes
            assert any("idx_orders_customer" in k for k in i.indexes)
            assert any("idx_orders_status" in k for k in i.indexes)

            # Functions
            assert any("calculate_order_total" in k for k in i.functions)
            assert any("update_order_total" in k for k in i.functions)

            # Triggers
            assert any("trg_update_order_total" in k for k in i.triggers)
            assert any("trg_audit_customers" in k for k in i.triggers)

            # Rules
            assert any("protect_shipped_orders" in k for k in i.rules)

            # Statistics
            assert any("stats_order_items_product_qty" in k for k in i.statistics)

            # Comments
            assert len(i.comments) >= 3

            # RLS policies
            assert len(i.rlspolicies) >= 1

            # Collations
            assert any("case_insensitive" in k for k in i.collations)

            # Text search dicts
            assert any("english_simple" in k for k in i.ts_dicts)

            # Text search configs
            assert any("english_custom" in k for k in i.ts_configs)
            config = [v for k, v in i.ts_configs.items() if "english_custom" in k][0]
            assert "asciiword" in config.mappings
            assert "word" in config.mappings

            # Event triggers
            assert '"evt_ddl_logger"' in i.event_triggers

            # Casts
            assert len(i.casts) >= 1
            cast_keys = list(i.casts.keys())
            assert any("positive_int" in k and "text" in k for k in cast_keys)

            # Operators
            assert any("<<>>" in k for k in i.operators)

            # Operator families
            assert any("custom_int_ops" in k for k in i.operator_families)

            # Operator classes
            assert any("custom_int_btree_ops" in k for k in i.operator_classes)
            opclass = [
                v for k, v in i.operator_classes.items() if "custom_int_btree_ops" in k
            ][0]
            assert len(opclass.operators) >= 5
            assert len(opclass.procs) >= 1

            # Publications
            assert '"pub_orders"' in i.publications
            pub = i.publications['"pub_orders"']
            assert len(pub.tables) == 2
