INSERT INTO silver.order_payments (
SELECT
    order_id,
    silver.safe_cast_integer(payment_sequential)    AS payment_sequential,
    payment_type,
    silver.safe_cast_integer(payment_installments)  AS payment_installments,
    silver.safe_cast_numeric(payment_value)          AS payment_value,
    %(target_snapshot_id)s,
    %(run_id)s::uuid,
    NOW(),
    'bronze.order_payments'
FROM bronze.order_payments
WHERE _snapshot_id = %(eff_order_payments)s
)