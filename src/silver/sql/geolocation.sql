INSERT INTO silver.geolocation (
SELECT DISTINCT ON (geolocation_zip_code_prefix)
    geolocation_zip_code_prefix,
    silver.safe_cast_double(geolocation_lat) AS geolocation_lat,
    silver.safe_cast_double(geolocation_lng) AS geolocation_lng,
    geolocation_city,
    UPPER(TRIM(geolocation_state)) AS geolocation_state,
    %(target_snapshot_id)s,
    %(run_id)s::uuid,
    NOW(),
    'bronze.geolocation'
FROM bronze.geolocation
WHERE _snapshot_id = %(eff_geolocation)s
ORDER BY geolocation_zip_code_prefix, _inserted_at DESC
)