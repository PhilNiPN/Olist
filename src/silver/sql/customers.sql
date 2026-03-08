INSERT INTO silver.customers (
SELECT
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    UPPER(TRIM(customer_state)) AS customer_state,
    %(target_snapshot_id)s,
    %(run_id)s::uuid,
    NOW(),
    'bronze.customers'
FROM bronze.customers
WHERE _snapshot_id = %(eff_customers)s
)