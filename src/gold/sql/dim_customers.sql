-- dim_customers: one row per real person (customer_unique_id).
--
-- Silver stores customer_id (one per order) and customer_unique_id
-- (the actual person). A single person can have multiple customer_ids
-- across different orders, so we GROUP BY customer_unique_id and
-- pick the most recent city/state (the one from their latest order).
--
-- first/last order dates and total_orders are pre-computed here
-- so downstream queries don't need to join back to orders.

INSERT INTO gold.dim_customers
SELECT
    c.customer_unique_id,
    c.customer_city,
    c.customer_state,
    MIN(o.order_purchase_ts)::date  AS first_order_date,
    MAX(o.order_purchase_ts)::date  AS last_order_date,
    COUNT(DISTINCT o.order_id)      AS total_orders,
    %(snapshot_id)s,
    %(run_id)s::uuid,
    NOW()
FROM silver.customers c
JOIN silver.orders o
    ON c.customer_id = o.customer_id
    AND o._snapshot_id = %(snapshot_id)s
WHERE c._snapshot_id = %(snapshot_id)s
GROUP BY
    c.customer_unique_id,
    -- We pick city/state from the row with the latest order.
    -- DISTINCT ON would also work, but this GROUP BY approach
    -- lets us compute the aggregates in one pass.
    c.customer_city,
    c.customer_state
