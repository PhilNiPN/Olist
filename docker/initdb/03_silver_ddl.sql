--- Silver schema

CREATE SCHEMA IF NOT EXISTS silver;


--- safe cast functions: returns NULL instead of raising error on bad data

CREATE OR REPLACE FUNCTION silver.safe_cast_timestamptz(val TEXT)
RETURNS TIMESTAMPTZ as $$ 
BEGIN 
    RETURN val::timestamptz;
EXCEPTION WHEN OTHERS THEN 
    RETURN NULL;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE FUNCTION silver.safe_cast_integer(val TEXT)
RETURNS INTEGER AS $$
BEGIN 
    RETURN val::integer;
EXCEPTION WHEN OTHERS THEN
    RETURN NULL;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE FUNCTION silver.safe_cast_numeric(val TEXT)
RETURNS NUMERIC AS $$
BEGIN 
    RETURN val::numeric;
EXCEPTION WHEN OTHERS THEN
    RETURN NULL;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE FUNCTION silver.safe_cast_double(val TEXT)
RETURNS DOUBLE PRECISION AS $$
BEGIN 
    RETURN val::double precision;
EXCEPTION WHEN OTHERS THEN
    RETURN NULL;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE FUNCTION silver.safe_cast_smallint(val TEXT)
RETURNS SMALLINT AS $$
BEGIN 
    RETURN val::smallint;
EXCEPTION WHEN OTHERS THEN
    RETURN NULL;
END;
$$ LANGUAGE plpgsql IMMUTABLE;


--- Lineage: this tracks which bronze snapshot fed each silver table

CREATE TABLE IF NOT EXISTS ingestion.silver_lineage (
    run_id                UUID NOT NULL REFERENCES ingestion.runs(run_id),
    silver_table          TEXT NOT NULL,
    bronze_table          TEXT NOT NULL,
    effective_snapshot_id TEXT NOT NULL,
    PRIMARY KEY (run_id, silver_table, bronze_table)
);
COMMENT ON TABLE ingestion.silver_lineage IS 'Maps each silver table load to the bronze snapshot it reads from.';


--- Silver tables

CREATE TABLE IF NOT EXISTS silver.orders (
    order_id              TEXT PRIMARY KEY,
    customer_id           TEXT NOT NULL,
    order_status          TEXT NOT NULL,
    order_purchase_ts     TIMESTAMPTZ NOT NULL,
    order_approved_at     TIMESTAMPTZ,
    delivered_carrier_at  TIMESTAMPTZ,
    delivered_customer_at TIMESTAMPTZ,
    estimated_delivery_at TIMESTAMPTZ,
    _snapshot_id          TEXT NOT NULL,
    _run_id               UUID NOT NULL, 
    _inserted_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    _source_table         TEXT NOT NULL
);
CREATE INDEX idx_silver_orders_snapshot ON silver.orders(_snapshot_id);

CREATE TABLE IF NOT EXISTS silver.order_items (
    order_id            TEXT NOT NULL,
    order_item_id       INTEGER NOT NULL,
    product_id          TEXT NOT NULL,
    seller_id           TEXT NOT NULL,
    shipping_limit_date TIMESTAMPTZ,
    price               NUMERIC(12,2) NOT NULL,
    freight_value       NUMERIC(12,2) NOT NULL,
    _snapshot_id        TEXT NOT NULL,
    _run_id             UUID NOT NULL, 
    _inserted_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    _source_table       TEXT NOT NULL,
    PRIMARY KEY (order_id, order_item_id)
);
CREATE INDEX idx_silver_order_items_snapshot ON silver.order_items(_snapshot_id);

CREATE TABLE IF NOT EXISTS silver.customers (
    customer_id              TEXT PRIMARY KEY,
    customer_unique_id       TEXT NOT NULL,
    customer_zip_code_prefix TEXT NOT NULL,
    customer_city            TEXT NOT NULL,
    customer_state           CHAR(2) NOT NULL,
    _snapshot_id             TEXT NOT NULL,
    _run_id                  UUID NOT NULL, 
    _inserted_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    _source_table            TEXT NOT NULL
);
CREATE INDEX idx_silver_customers_snapshot ON silver.customers(_snapshot_id);

CREATE TABLE IF NOT EXISTS silver.products (
    product_id                 TEXT PRIMARY KEY,
    product_category_name      TEXT,
    product_category_name_en   TEXT,
    product_name_length        INTEGER,
    product_description_length INTEGER,
    product_photos_qty         INTEGER,
    product_weight_g           INTEGER,
    product_length_cm          INTEGER,
    product_height_cm          INTEGER,
    product_width_cm           INTEGER,
    _snapshot_id               TEXT NOT NULL,
    _run_id                    UUID NOT NULL, 
    _inserted_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    _source_table              TEXT NOT NULL
);
CREATE INDEX idx_silver_products_snapshot ON silver.products(_snapshot_id);

CREATE TABLE IF NOT EXISTS silver.sellers (
    seller_id              TEXT PRIMARY KEY,
    seller_zip_code_prefix TEXT NOT NULL,
    seller_city            TEXT NOT NULL,
    seller_state           CHAR(2) NOT NULL,
    _snapshot_id           TEXT NOT NULL,
    _run_id                UUID NOT NULL, 
    _inserted_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    _source_table          TEXT NOT NULL
);
CREATE INDEX idx_silver_sellers_snapshot ON silver.sellers(_snapshot_id);

CREATE TABLE IF NOT EXISTS silver.order_reviews (
    review_id              TEXT NOT NULL,
    order_id               TEXT NOT NULL,
    review_score           SMALLINT NOT NULL,
    review_comment_title   TEXT,
    review_comment_message TEXT,
    review_creation_date   TIMESTAMPTZ NOT NULL,
    review_answer_ts       TIMESTAMPTZ NOT NULL,
    _snapshot_id           TEXT NOT NULL,
    _run_id                UUID NOT NULL, 
    _inserted_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    _source_table          TEXT NOT NULL,
    PRIMARY KEY (review_id, order_id)
);
CREATE INDEX idx_silver_order_reviews_snapshot ON silver.order_reviews(_snapshot_id);

CREATE TABLE IF NOT EXISTS silver.order_payments (
    order_id             TEXT NOT NULL,
    payment_sequential   INTEGER NOT NULL,
    payment_type         TEXT NOT NULL,
    payment_installments INTEGER NOT NULL,
    payment_value        NUMERIC(12,2) NOT NULL,
    _snapshot_id         TEXT NOT NULL,
    _run_id              UUID NOT NULL, 
    _inserted_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    _source_table        TEXT NOT NULL,
    PRIMARY KEY (order_id, payment_sequential)
);
CREATE INDEX idx_silver_order_payments_snapshot ON silver.order_payments(_snapshot_id);

CREATE TABLE IF NOT EXISTS silver.geolocation (
    geolocation_zip_code_prefix TEXT PRIMARY KEY,
    geolocation_lat             DOUBLE PRECISION NOT NULL,
    geolocation_lng             DOUBLE PRECISION NOT NULL,
    geolocation_city            TEXT NOT NULL,
    geolocation_state           CHAR(2) NOT NULL,
    _snapshot_id                TEXT NOT NULL,
    _run_id                     UUID NOT NULL, 
    _inserted_at                TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    _source_table               TEXT NOT NULL
);
CREATE INDEX idx_silver_geolocation_snapshot ON silver.geolocation(_snapshot_id);

COMMENT ON SCHEMA silver IS 'Cleaned and typed data layer. Columns cast to proper types, typos fixed, translation table merged into products, geolocation deduplicated.';