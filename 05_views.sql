-- Useful materialized views for fast analytics + a convenience refresh function.

SET search_path = nyc_taxi, public;

-- 1) Monthly fare-per-mile (one row per month)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_monthly_fare_per_mile AS
SELECT
  date_trunc('month', tpep_pickup_datetime)::date AS month,
  AVG(total_amount / NULLIF(trip_distance,0))    AS avg_fare_per_mile,
  COUNT(*)                                       AS trips
FROM nyc_taxi.trips
GROUP BY 1;

-- Unique index required for CONCURRENT refresh
CREATE UNIQUE INDEX IF NOT EXISTS ux_mv_monthly_fpm_month
  ON mv_monthly_fare_per_mile (month);

-- 2) Route revenue (PU->DO)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_route_revenue AS
SELECT
  pulocationid,
  dolocationid,
  SUM(total_amount) AS revenue,
  COUNT(*)          AS trips
FROM nyc_taxi.trips
GROUP BY 1,2;

-- Unique index for CONCURRENT refresh (one row per route)
CREATE UNIQUE INDEX IF NOT EXISTS ux_mv_route_revenue_route
  ON mv_route_revenue (pulocationid, dolocationid);

-- 3) Demand by hour of day (0..23)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_demand_by_hour AS
SELECT
  EXTRACT(HOUR FROM tpep_pickup_datetime)::int AS hour_of_day,
  COUNT(*)                                     AS trips
FROM nyc_taxi.trips
GROUP BY 1;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mv_demand_by_hour
  ON mv_demand_by_hour (hour_of_day);

-- 4) Tip% by payment type
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_tip_pct_by_payment AS
SELECT
  payment_type,
  AVG(CASE WHEN fare_amount > 0 THEN tip_amount/fare_amount END) AS avg_tip_pct,
  COUNT(*) AS trips
FROM nyc_taxi.trips
GROUP BY 1;

CREATE UNIQUE INDEX IF NOT EXISTS ux_mv_tip_pct_by_payment
  ON mv_tip_pct_by_payment (payment_type);

-- (Optional) convenience plain views that join zone names for readability
CREATE OR REPLACE VIEW v_route_revenue_named AS
SELECT
  pu.borough  AS pu_borough,
  pu.zone     AS pu_zone,
  doo.borough AS do_borough,
  doo.zone    AS do_zone,
  r.revenue,
  r.trips
FROM mv_route_revenue r
LEFT JOIN nyc_taxi.zones pu  ON pu.location_id = r.pulocationid
LEFT JOIN nyc_taxi.zones doo ON doo.location_id = r.dolocationid;

-- 5) One-button refresh for all MVs (non-blocking to readers)
CREATE OR REPLACE FUNCTION refresh_all_mvs() RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
  REFRESH MATERIALIZED VIEW CONCURRENTLY mv_monthly_fare_per_mile;
  REFRESH MATERIALIZED VIEW CONCURRENTLY mv_route_revenue;
  REFRESH MATERIALIZED VIEW CONCURRENTLY mv_demand_by_hour;
  REFRESH MATERIALIZED VIEW CONCURRENTLY mv_tip_pct_by_payment;
END$$;

-- 6) Helpful indexes on MVs for common sorts/filters (optional; they’re small anyway)
CREATE INDEX IF NOT EXISTS ix_mv_monthly_fpm_trips
  ON mv_monthly_fare_per_mile (trips DESC);

CREATE INDEX IF NOT EXISTS ix_mv_route_revenue_rev
  ON mv_route_revenue (revenue DESC);

CREATE INDEX IF NOT EXISTS ix_mv_demand_by_hour_trips
  ON mv_demand_by_hour (trips DESC);
