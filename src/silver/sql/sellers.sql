INSERT INTO silver.sellers (
SELECT
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    UPPER(TRIM(seller_state)) AS seller_state,
    %(target_snapshot_id)s,
    %(run_id)s::uuid,
    NOW(),
    'bronze.sellers'
FROM bronze.sellers
WHERE _snapshot_id = %(eff_sellers)s
)