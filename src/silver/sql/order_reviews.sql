INSERT INTO silver.order_reviews (
SELECT
    review_id,
    order_id,
    silver.safe_cast_smallint(review_score)               AS review_score,
    review_comment_title,
    review_comment_message,
    silver.safe_cast_timestamptz(review_creation_date)    AS review_creation_date,
    silver.safe_cast_timestamptz(review_answer_timestamp) AS review_answer_ts,
    %(target_snapshot_id)s,
    %(run_id)s::uuid,
    NOW(),
    'bronze.order_reviews'
FROM bronze.order_reviews
WHERE _snapshot_id = %(eff_order_reviews)s
)