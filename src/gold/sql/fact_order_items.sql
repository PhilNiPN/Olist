-- fact_order_items: one row per (order_id, order_item_id).
--
-- This is the central fact table. It joins five silver tables:
--   - order_items  (grain source — one row per item in an order)
--   - orders       (order context: status, timestamps)
--   - customers    (to resolve customer_unique_id from customer_id)
--   - order_reviews (LEFT JOIN — not every order has a review)
--   - order_payments (aggregated per order to get dominant payment type)
--
-- Payment denormalization strategy:
--   Payments live at (order_id, payment_sequential) grain, but the fact
--   is at item grain. We pick the "dominant" payment type = the one with
--   the highest total value on the order (via the pay CTE). This is a
--   pragmatic simplification; a separate fact_payments table can be
--   added later for detailed payment-mix analysis.
--
-- Delivery metrics:
--   days_to_deliver, days_delivery_vs_estimate, and delivered_on_time
--   are only meaningful for delivered orders — they'll be NULL otherwise.

INSERT INTO gold.fact_order_items
WITH pay AS (
    -- Aggregate payments per order: pick the payment type that accounts
    -- for the largest share of the order value, and the max installments.
    SELECT DISTINCT ON (order_id)
        order_id,
        payment_type,
        MAX(payment_installments) OVER (PARTITION BY order_id) AS payment_installments
    FROM silver.order_payments
    WHERE _snapshot_id = %(snapshot_id)s
    ORDER BY order_id, payment_value DESC
),
rev AS (
    -- Orders can have multiple reviews; take the latest one per order.
    SELECT DISTINCT ON (order_id)
        order_id,
        review_score
    FROM silver.order_reviews
    WHERE _snapshot_id = %(snapshot_id)s
    ORDER BY order_id, review_answer_ts DESC
)
SELECT
    oi.order_id,
    oi.order_item_id,

    -- Resolve the real person behind the per-order customer_id
    c.customer_unique_id,
    oi.product_id,
    oi.seller_id,
    o.order_purchase_ts::date                           AS order_purchase_date,

    o.order_status,

    oi.price,
    oi.freight_value,
    oi.price + oi.freight_value                         AS total_value,

    pay.payment_type,
    pay.payment_installments,

    rev.review_score,

    o.order_approved_at,
    o.delivered_carrier_at,
    o.delivered_customer_at,
    o.estimated_delivery_at,

    -- Delivery metrics: only populated when the order was actually delivered
    CASE WHEN o.delivered_customer_at IS NOT NULL
         THEN (o.delivered_customer_at::date - o.order_purchase_ts::date)
    END                                                 AS days_to_deliver,

    CASE WHEN o.delivered_customer_at IS NOT NULL AND o.estimated_delivery_at IS NOT NULL
         THEN (o.delivered_customer_at::date - o.estimated_delivery_at::date)
    END                                                 AS days_delivery_vs_estimate,

    CASE WHEN o.delivered_customer_at IS NOT NULL AND o.estimated_delivery_at IS NOT NULL
         THEN o.delivered_customer_at <= o.estimated_delivery_at
    END                                                 AS delivered_on_time,

    %(snapshot_id)s,
    %(run_id)s::uuid,
    NOW()

FROM silver.order_items oi
JOIN silver.orders o
    ON oi.order_id = o.order_id
    AND o._snapshot_id = %(snapshot_id)s
JOIN silver.customers c
    ON o.customer_id = c.customer_id
    AND c._snapshot_id = %(snapshot_id)s
LEFT JOIN pay
    ON oi.order_id = pay.order_id
LEFT JOIN rev
    ON oi.order_id = rev.order_id
WHERE oi._snapshot_id = %(snapshot_id)s
