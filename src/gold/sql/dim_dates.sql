-- dim_dates: generate a calendar row for every date between the earliest
-- and latest order in silver.orders for this snapshot.
--
-- We use generate_series so the dimension covers the full date range
-- without gaps, even if no orders exist on a particular day.
-- This avoids the "missing date" problem in time-series dashboards.

INSERT INTO gold.dim_dates
SELECT
    d.date_key,
    EXTRACT(YEAR   FROM d.date_key)::SMALLINT       AS year,
    EXTRACT(QUARTER FROM d.date_key)::SMALLINT      AS quarter,
    EXTRACT(MONTH  FROM d.date_key)::SMALLINT       AS month,
    TO_CHAR(d.date_key, 'FMMonth')                  AS month_name,
    EXTRACT(WEEK   FROM d.date_key)::SMALLINT       AS week_of_year,
    EXTRACT(DOW    FROM d.date_key)::SMALLINT       AS day_of_week,
    TO_CHAR(d.date_key, 'FMDay')                    AS day_name,
    EXTRACT(DOW FROM d.date_key) IN (0, 6)          AS is_weekend,
    TO_CHAR(d.date_key, 'YYYY-MM')                  AS year_month
FROM (
    SELECT generate_series(
        (SELECT MIN(order_purchase_ts)::date FROM silver.orders WHERE _snapshot_id = %(snapshot_id)s),
        (SELECT MAX(order_purchase_ts)::date FROM silver.orders WHERE _snapshot_id = %(snapshot_id)s),
        '1 day'::interval
    )::date AS date_key
) d
-- ON CONFLICT so re-runs don't fail on the PK (dates are static once generated)
ON CONFLICT (date_key) DO NOTHING
