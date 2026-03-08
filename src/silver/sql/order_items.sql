INSERT INTO silver.order_items (
SELECT
    order_id,
    silver.safe_cast_integer(order_item_id)        AS order_item_id,
    product_id,
    seller_id,
    silver.safe_cast_timestamptz(shipping_limit_date) AS shipping_limit_date,
    silver.safe_cast_numeric(price)                 AS price,
    silver.safe_cast_numeric(freight_value)          AS freight_value,
    %(target_snapshot_id)s,
    %(run_id)s::uuid,
    NOW(),
    'bronze.order_items'
FROM bronze.order_items
WHERE _snapshot_id = %(eff_order_items)s
)