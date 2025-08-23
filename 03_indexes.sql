SET search_path = nyc_taxi, public;

-- Time filtering / grouping
CREATE INDEX IF NOT EXISTS idx_trips_pickup_ts_btree
  ON trips (tpep_pickup_datetime);
CREATE INDEX IF NOT EXISTS idx_trips_dropoff_ts_btree
  ON trips (tpep_dropoff_datetime);

-- Big scans over time (BRIN = tiny, fast for ranged scans)
CREATE INDEX IF NOT EXISTS idx_trips_pickup_ts_brin
  ON trips USING BRIN (tpep_pickup_datetime) WITH (pages_per_range = 128);

-- Month bucket (helps WHERE date_trunc('month', ...) = ...)
CREATE INDEX IF NOT EXISTS idx_trips_pickup_month
  ON trips ((date_trunc('month', tpep_pickup_datetime)));

-- Route revenue (index-only scans for SUM(total_amount))
CREATE INDEX IF NOT EXISTS idx_trips_route_revenue
  ON trips (pulocationid, dolocationid) INCLUDE (total_amount);

-- Time + zone (common filter: recent month + PU zone)
CREATE INDEX IF NOT EXISTS idx_trips_pickup_time_pu
  ON trips (tpep_pickup_datetime, pulocationid);
CREATE INDEX IF NOT EXISTS idx_trips_pickup_time_do
  ON trips (tpep_pickup_datetime, dolocationid);

-- Tip % by payment type (cover fare/tip)
CREATE INDEX IF NOT EXISTS idx_trips_payment_tip
  ON trips (payment_type) INCLUDE (fare_amount, tip_amount);

-- Distance filters/bucketing
CREATE INDEX IF NOT EXISTS idx_trips_distance
  ON trips (trip_distance);

-- Surcharges / airport (fast WHERE with partial indexes)
CREATE INDEX IF NOT EXISTS idx_trips_airport_fee
  ON trips (pulocationid, dolocationid, tpep_pickup_datetime)
  WHERE airport_fee IS NOT NULL AND airport_fee > 0;

CREATE INDEX IF NOT EXISTS idx_trips_congestion
  ON trips (tpep_pickup_datetime, pulocationid)
  WHERE congestion_surcharge IS NOT NULL AND congestion_surcharge > 0;

-- Planning accuracy (raise stats on skewed cols)
ALTER TABLE trips ALTER COLUMN pulocationid SET STATISTICS 500;
ALTER TABLE trips ALTER COLUMN dolocationid SET STATISTICS 500;
ALTER TABLE trips ALTER COLUMN tpep_pickup_datetime SET STATISTICS 500;

-- Refresh stats
ANALYZE trips;

-- cluster physical order by time (run during low-traffic)
-- CLUSTER trips USING idx_trips_pickup_ts_btree;
-- VACUUM (ANALYZE) trips;
