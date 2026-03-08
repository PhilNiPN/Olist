INSERT INTO silver.orders (
    SELECT order_id,
        customer_id,
        order_status,
        silver.safe_cast_timestamptz(order_purchase_timestamp) AS order_purchase_ts,
        silver.safe_cast_timestamptz(order_approved_at) AS order_approved_ts,
        silver.safe_cast_timestamptz(order_delivered_carrier_date) AS delivered_carrier_at,
        silver.safe_cast_timestamptz(order_delivered_customer_date) AS delivered_customer_at,
        silver.safe_cast_timestamptz(order_estimated_delivery_date) AS estimated_delivery_at,
        %(target_snapshot_id)s,
        %(run_id)s::uuid,
        NOW(),
        'bronze.orders'
    FROM bronze.orders
    WHERE _snapshot_id = %(eff_orders)s
)