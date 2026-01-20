-- helper to create standardized bronze columns
-- NOTE: business data is TEXT to prevent load failures

CREATE TABLE IF NOT EXISTS bronze.orders (
    order_id TEXT, 
    customer_id TEXT, 
    order_status TEXT,
    order_purchase_timestamp TEXT,
    order_approved_at TEXT,
    order_delivered_carrier_date TEXT,
    order_dilivered_customer_date TEXT,
    order_estimated_delivery_date TEXT,
    _snapshot_id TEXT,
    _run_id UUID,
    _inserted_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX idx_bronze_orders_snapshot ON bronze.orders(_snapshot_id);

CREATE TABLE IF NOT EXISTS bronze.order_items (
    order_id TEXT,
    order_item_id TEXT,
    product_id TEXT,
    seller_id TEXT,
    shipping_limit_date TEXT,
    price TEXT,
    freight_value TEXT,
    _snapshot_id TEXT,
    _run_id UUID,
    _inserted_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX idx_bronze_order_items_snapshot ON bronze.order_items(_snapshot_id);

CREATE TABLE IF NOT EXISTS bronze.customers (
    customer_id TEXT,
    customer_unique_id TEXT,
    customer_zip_code_prefix TEXT,
    customer_city TEXT,
    customer_state TEXT,
    _snapshot_id TEXT,
    _run_id UUID,
    _inserted_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX idx_bronze_customers_snapshot ON bronze.customers(_snapshot_id);

CREATE TABLE IF NOT EXISTS bronze.products (
    product_id TEXT, 
    product_category_name TEXT,
    product_name_length TEXT,
    product_description_length TEXT,
    product_photos_qty TEXT,
    product_weight_g TEXT,
    product_length_cm TEXT,
    product_height_cm TEXT,
    product_width_cm TEXT,
    _snapshot_id TEXT,
    _run_id UUID,
    _inserted_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX idx_bronze_products_snapshot ON bronze.products(_snapshot_id);

CREATE TABLE IF NOT EXISTS bronze.sellers (
    seller_id TEXT, 
    seller_zip_code_prefix TEXT,
    seller_city TEXT,
    seller_state TEXT,
    _snapshot_id TEXT,
    _run_id UUID,
    _inserted_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX idx_bronze_sellers_snapshot ON bronze.sellers(_snapshot_id);

CREATE TABLE IF NOT EXISTS bronze.order_reviews (
    review_id TEXT, 
    order_id TEXT,
    review_score TEXT,
    review_comment_title TEXT, 
    review_comment_message TEXT,
    review_creation_date TEXT,
    review_answer_timestamp TEXT,
    _snapshot_id TEXT,
    _run_id UUID,
    _inserted_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX idx_bronze_order_reviews_snapshot ON bronze.order_reviews(_snapshot_id);

CREATE TABLE IF NOT EXISTS bronze.order_payments (
    order_id TEXT,
    payment_sequential TEXT,
    payment_type TEXT,
    payment_installments TEXT,
    payment_value TEXT,
    _snapshot_id TEXT,
    _run_id UUID,
    _inserted_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX idx_bronze_order_payments_snapshot ON bronze.order_payments(_snapshot_id);

CREATE TABLE IF NOT EXISTS bronze.geolocation (
    geolocation_zip_code_prefix TEXT,
    geolocation_lat TEXT,
    geolocation_lng TEXT,
    geolocation_city TEXT,
    geolocation_state TEXT,
    _snapshot_id TEXT,
    _run_id UUID,
    _inserted_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX idx_bronze_geolocation_snapshot ON bronze.geolocation(_snapshot_id);

CREATE TABLE IF NOT EXISTS bronze.product_category_name_translation (
    product_category_name TEXT,
    product_category_name_english TEXT,
    _snapshot_id TEXT,
    _run_id UUID,
    _inserted_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX idx_bronze_prod_cat_translation_snapshot ON bronze.product_category_name_translation(_snapshot_id);
