-- Gold schema: business-ready star schema built from silver.
-- Dimensions are loaded first (they have no FKs into other gold tables),
-- then the fact table joins across them.

CREATE SCHEMA IF NOT EXISTS gold;


-- ============================================================
-- Ingestion tracking: mirrors silver_table_loads for the gold layer
-- so every gold run is auditable through the same ingestion schema.
-- ============================================================

CREATE TABLE IF NOT EXISTS ingestion.gold_table_loads (
    run_id        UUID NOT NULL REFERENCES ingestion.runs(run_id),
    gold_table    TEXT NOT NULL,
    status        TEXT NOT NULL CHECK (status IN ('pending', 'loaded', 'failed', 'dq_rejected')),
    rows_inserted BIGINT DEFAULT 0,
    message       TEXT,
    updated_at    TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (run_id, gold_table)
);
COMMENT ON TABLE ingestion.gold_table_loads IS 'Tracks load status of each gold table within a pipeline run.';


-- Lineage: which silver snapshot fed each gold table.
-- Same idea as ingestion.silver_lineage but one layer up.

CREATE TABLE IF NOT EXISTS ingestion.gold_lineage (
    run_id                UUID NOT NULL REFERENCES ingestion.runs(run_id),
    gold_table            TEXT NOT NULL,
    silver_table          TEXT NOT NULL,
    effective_snapshot_id  TEXT NOT NULL,
    PRIMARY KEY (run_id, gold_table, silver_table)
);
COMMENT ON TABLE ingestion.gold_lineage IS 'Maps each gold table load to the silver snapshot it reads from.';


-- ============================================================
-- dim_dates: calendar dimension covering every date in the dataset.
-- Generated from the date range in silver.orders so it stays
-- in sync with the data without hardcoded bounds.
-- ============================================================

CREATE TABLE IF NOT EXISTS gold.dim_dates (
    date_key      DATE PRIMARY KEY,
    year          SMALLINT NOT NULL,
    quarter       SMALLINT NOT NULL,
    month         SMALLINT NOT NULL,
    month_name    TEXT NOT NULL,
    week_of_year  SMALLINT NOT NULL,
    day_of_week   SMALLINT NOT NULL,     -- 0=Sun, 6=Sat (extract DOW)
    day_name      TEXT NOT NULL,
    is_weekend    BOOLEAN NOT NULL,
    year_month    TEXT NOT NULL           -- '2017-09' for easy GROUP BY
);
CREATE INDEX IF NOT EXISTS idx_gold_dim_dates_year_month ON gold.dim_dates(year_month);


-- ============================================================
-- dim_customers: one row per real person (customer_unique_id).
-- Silver has customer_id (per-order surrogate) and customer_unique_id
-- (the actual person). We grain on unique_id so repeat-purchase
-- analysis works correctly.
-- ============================================================

CREATE TABLE IF NOT EXISTS gold.dim_customers (
    customer_unique_id TEXT PRIMARY KEY,
    customer_city      TEXT NOT NULL,
    customer_state     CHAR(2) NOT NULL,
    first_order_date   DATE,               -- earliest purchase date
    last_order_date    DATE,                -- most recent purchase date
    total_orders       INTEGER NOT NULL,    -- count of distinct orders
    _snapshot_id       TEXT NOT NULL,
    _run_id            UUID NOT NULL,
    _inserted_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_gold_dim_customers_snapshot ON gold.dim_customers(_snapshot_id);
CREATE INDEX IF NOT EXISTS idx_gold_dim_customers_state ON gold.dim_customers(customer_state);


-- ============================================================
-- dim_products: one row per product_id.
-- Includes the English category name (already resolved in silver
-- via the translation table) and a computed volume column
-- to avoid recalculating L*H*W in every query.
-- ============================================================

CREATE TABLE IF NOT EXISTS gold.dim_products (
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
    product_volume_cm3         INTEGER,          -- length * height * width
    _snapshot_id               TEXT NOT NULL,
    _run_id                    UUID NOT NULL,
    _inserted_at               TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_gold_dim_products_snapshot ON gold.dim_products(_snapshot_id);
CREATE INDEX IF NOT EXISTS idx_gold_dim_products_category ON gold.dim_products(product_category_name_en);


-- ============================================================
-- dim_sellers: one row per seller_id.
-- Carries geography for seller-location analytics.
-- ============================================================

CREATE TABLE IF NOT EXISTS gold.dim_sellers (
    seller_id    TEXT PRIMARY KEY,
    seller_city  TEXT NOT NULL,
    seller_state CHAR(2) NOT NULL,
    _snapshot_id TEXT NOT NULL,
    _run_id      UUID NOT NULL,
    _inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_gold_dim_sellers_snapshot ON gold.dim_sellers(_snapshot_id);
CREATE INDEX IF NOT EXISTS idx_gold_dim_sellers_state ON gold.dim_sellers(seller_state);


-- ============================================================
-- fact_order_items: the central fact table at order-item grain.
--
-- Why this grain? It is the most granular useful level: one row
-- per item purchased. Every higher-level aggregate (order, customer,
-- seller, category, month) can be rolled up from here.
--
-- Denormalized fields (review_score, delivery dates, payment info)
-- are pulled in so common analytical queries don't need multi-way
-- joins — the star schema handles the rest via dimension FKs.
-- ============================================================

CREATE TABLE IF NOT EXISTS gold.fact_order_items (
    order_id                TEXT NOT NULL,
    order_item_id           INTEGER NOT NULL,

    -- dimension FKs
    customer_unique_id      TEXT NOT NULL,
    product_id              TEXT NOT NULL,
    seller_id               TEXT NOT NULL,
    order_purchase_date     DATE NOT NULL,          -- FK to dim_dates

    -- order context
    order_status            TEXT NOT NULL,

    -- money
    price                   NUMERIC(12,2) NOT NULL,
    freight_value           NUMERIC(12,2) NOT NULL,
    total_value             NUMERIC(12,2) NOT NULL,  -- price + freight

    -- payment (order-level, denormalized onto each item)
    payment_type            TEXT,                     -- dominant payment method
    payment_installments    INTEGER,                  -- max installments on the order

    -- satisfaction
    review_score            SMALLINT,                 -- NULL if no review yet

    -- delivery timestamps (from silver.orders)
    order_approved_at       TIMESTAMPTZ,
    delivered_carrier_at    TIMESTAMPTZ,
    delivered_customer_at   TIMESTAMPTZ,
    estimated_delivery_at   TIMESTAMPTZ,

    -- computed delivery metrics (only meaningful for delivered orders)
    days_to_deliver         INTEGER,                  -- delivered_customer - purchase
    days_delivery_vs_estimate INTEGER,                -- delivered_customer - estimated (neg = early)
    delivered_on_time       BOOLEAN,                  -- true if delivered <= estimated

    -- lineage
    _snapshot_id            TEXT NOT NULL,
    _run_id                 UUID NOT NULL,
    _inserted_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (order_id, order_item_id)
);
CREATE INDEX IF NOT EXISTS idx_gold_fact_oi_snapshot ON gold.fact_order_items(_snapshot_id);
CREATE INDEX IF NOT EXISTS idx_gold_fact_oi_purchase_date ON gold.fact_order_items(order_purchase_date);
CREATE INDEX IF NOT EXISTS idx_gold_fact_oi_customer ON gold.fact_order_items(customer_unique_id);
CREATE INDEX IF NOT EXISTS idx_gold_fact_oi_product ON gold.fact_order_items(product_id);
CREATE INDEX IF NOT EXISTS idx_gold_fact_oi_seller ON gold.fact_order_items(seller_id);


COMMENT ON SCHEMA gold IS 'Business-ready star schema. Dimensions + fact table at order-item grain, built from silver layer.';
