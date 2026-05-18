-- dim_sellers: one row per seller_id, straight from silver.
-- Geography (city/state) enables seller-location based analytics.

INSERT INTO gold.dim_sellers
SELECT
    seller_id,
    seller_city,
    seller_state,
    %(snapshot_id)s,
    %(run_id)s::uuid,
    NOW()
FROM silver.sellers
WHERE _snapshot_id = %(snapshot_id)s
