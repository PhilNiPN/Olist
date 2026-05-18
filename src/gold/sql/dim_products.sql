-- dim_products: one row per product_id, straight from silver.
--
-- The English category name was already resolved in the silver layer
-- (via the translation table join). We add product_volume_cm3 as a
-- convenience column so consumers don't have to multiply L*H*W themselves.
-- NULL dimensions yield NULL volume — COALESCE is intentionally avoided
-- because a zero-volume product is misleading; NULL means "unknown".

INSERT INTO gold.dim_products
SELECT
    product_id,
    product_category_name,
    product_category_name_en,
    product_name_length,
    product_description_length,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm,
    product_length_cm * product_height_cm * product_width_cm  AS product_volume_cm3,
    %(snapshot_id)s,
    %(run_id)s::uuid,
    NOW()
FROM silver.products
WHERE _snapshot_id = %(snapshot_id)s
