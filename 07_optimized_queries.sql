SET search_path = nyc_taxi, public;
\timing on

-- Optional: refresh precomputed aggregates (non-blocking to readers)
-- SELECT refresh_all_mvs();

-- Keep the same window as baseline for apples-to-apples where applicable
\set start '2023-01-01'
\set end   '2023-07-01'

-- A) Avg fare per mile by month (tiny MV scan)
EXPLAIN (ANALYZE, BUFFERS)
SELECT month, avg_fare_per_mile, trips
FROM mv_monthly_fare_per_mile
ORDER BY month;

-- A2) Same, windowed
EXPLAIN (ANALYZE, BUFFERS)
SELECT month, avg_fare_per_mile, trips
FROM mv_monthly_fare_per_mile
WHERE month >= :'start'::date
  AND month <  :'end'::date
ORDER BY month;

-- B) Top revenue routes (pre-aggregated)
EXPLAIN (ANALYZE, BUFFERS)
SELECT pulocationid, dolocationid, revenue, trips
FROM mv_route_revenue
ORDER BY revenue DESC
LIMIT 10;

-- B2) Named version (joins are on small dimension + MV)
EXPLAIN (ANALYZE, BUFFERS)
SELECT *
FROM v_route_revenue_named
ORDER BY revenue DESC
LIMIT 10;

-- C) Peak demand hours (MV)
EXPLAIN (ANALYZE, BUFFERS)
SELECT hour_of_day, trips
FROM mv_demand_by_hour
ORDER BY trips DESC;

-- D) Tip% by payment type (MV)
EXPLAIN (ANALYZE, BUFFERS)
SELECT payment_type, avg_tip_pct, trips
FROM mv_tip_pct_by_payment
ORDER BY avg_tip_pct DESC;
