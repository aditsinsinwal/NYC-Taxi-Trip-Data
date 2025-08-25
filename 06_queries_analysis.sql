SET search_path = nyc_taxi, public;
\timing on

-- Optional: define a time window for pruning tests (psql variables)
\set start '2023-01-01'
\set end   '2023-07-01'

-- A) Avg fare per mile by month (full table scan/group)
EXPLAIN (ANALYZE, BUFFERS)
SELECT date_trunc('month', tpep_pickup_datetime) AS month,
       AVG(total_amount / NULLIF(trip_distance,0)) AS avg_fare_per_mile,
       COUNT(*) AS trips
FROM trips
GROUP BY 1
ORDER BY 1;

-- A2) Same, but within a time window (should prune to a few partitions)
EXPLAIN (ANALYZE, BUFFERS)
SELECT date_trunc('month', tpep_pickup_datetime) AS month,
       AVG(total_amount / NULLIF(trip_distance,0)) AS avg_fare_per_mile,
       COUNT(*) AS trips
FROM trips
WHERE tpep_pickup_datetime >= :'start'::timestamp
  AND tpep_pickup_datetime <  :'end'::timestamp
GROUP BY 1
ORDER BY 1;

-- B) Top revenue routes (route = PU->DO)
EXPLAIN (ANALYZE, BUFFERS)
SELECT pulocationid, dolocationid, SUM(total_amount) AS revenue, COUNT(*) AS trips
FROM trips
GROUP BY 1,2
ORDER BY revenue DESC
LIMIT 10;

-- B2) Same but with zone names (double self-join on zones)
EXPLAIN (ANALYZE, BUFFERS)
SELECT pu.borough  AS pu_borough,
       pu.zone     AS pu_zone,
       doo.borough AS do_borough,
       doo.zone    AS do_zone,
       SUM(t.total_amount) AS revenue,
       COUNT(*) AS trips
FROM trips t
JOIN zones pu  ON pu.location_id  = t.pulocationid
JOIN zones doo ON doo.location_id = t.dolocationid
GROUP BY 1,2,3,4
ORDER BY revenue DESC
LIMIT 10;

-- C) Peak demand hours (0..23)
EXPLAIN (ANALYZE, BUFFERS)
SELECT EXTRACT(HOUR FROM tpep_pickup_datetime)::int AS hour_of_day,
       COUNT(*) AS trips
FROM trips
GROUP BY 1
ORDER BY trips DESC;

-- D) Tip% by payment type
EXPLAIN (ANALYZE, BUFFERS)
SELECT payment_type,
       AVG(CASE WHEN fare_amount > 0 THEN tip_amount/fare_amount END) AS avg_tip_pct,
       COUNT(*) AS trips
FROM trips
GROUP BY 1
ORDER BY avg_tip_pct DESC;

-- E) Airport-related revenue by month (tests partial index & pruning)
EXPLAIN (ANALYZE, BUFFERS)
SELECT date_trunc('month', tpep_pickup_datetime) AS month,
       SUM(total_amount) AS revenue,
       COUNT(*) AS trips
FROM trips
WHERE airport_fee IS NOT NULL AND airport_fee > 0
GROUP BY 1
ORDER BY 1;

-- F) Daily trips + 7-day rolling average (window function baseline)
EXPLAIN (ANALYZE, BUFFERS)
WITH daily AS (
  SELECT date_trunc('day', tpep_pickup_datetime)::date AS d,
         COUNT(*) AS trips
  FROM trips
  GROUP BY 1
)
SELECT d,
       trips,
       AVG(trips) OVER (ORDER BY d ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS trips_7d_avg
FROM daily
ORDER BY d;

-- G) One-month heavy route revenue (explicit month filter for pruning)
EXPLAIN (ANALYZE, BUFFERS)
SELECT pulocationid, dolocationid, SUM(total_amount) AS revenue
FROM trips
WHERE tpep_pickup_datetime >= :'start'::timestamp
  AND tpep_pickup_datetime <  :'end'::timestamp
GROUP BY 1,2
ORDER BY revenue DESC
LIMIT 15;
