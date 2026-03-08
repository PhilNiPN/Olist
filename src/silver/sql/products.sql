INSERT INTO silver.products (
SELECT
    p.product_id,
    p.product_category_name,
    t.product_category_name_english                            AS product_category_name_en,
    silver.safe_cast_integer(p.product_name_lenght)            AS product_name_length,
    silver.safe_cast_integer(p.product_description_lenght)     AS product_description_length,
    silver.safe_cast_integer(p.product_photos_qty)             AS product_photos_qty,
    silver.safe_cast_integer(p.product_weight_g)               AS product_weight_g,
    silver.safe_cast_integer(p.product_length_cm)              AS product_length_cm,
    silver.safe_cast_integer(p.product_height_cm)              AS product_height_cm,
    silver.safe_cast_integer(p.product_width_cm)               AS product_width_cm,
    %(target_snapshot_id)s,
    %(run_id)s::uuid,
    NOW(),
    'bronze.products'
FROM bronze.products p
LEFT JOIN bronze.product_category_name_translation t
    ON  p.product_category_name = t.product_category_name
    AND t._snapshot_id = %(eff_product_category_name_translation)s
WHERE p._snapshot_id = %(eff_products)s
)