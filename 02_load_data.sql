-- CSV files from Kaggle are extracted locally.
-- Use \copy (psql client) for local files; or COPY with a server-side path.

SET search_path = nyc_taxi, public;

-- Loading zones lookup (taxi_zone_lookup.csv)
-- Replace PATH below with your actual file path.
\copy nyc_taxi.zones (location_id, borough, zone, service_zone)
FROM '/path/to/taxi_zone_lookup.csv' CSV HEADER;

-- Loading one or more monthly yellow taxi CSVs into staging, then cast into trips.
-- Example for one month file: yellow_tripdata_2023-01.csv
-- Repeat these two steps per file you want to ingest.

--  Truncate staging before each file
TRUNCATE TABLE nyc_taxi.staging_trips;

-- Fast load raw CSV into staging (adjust path)
\copy nyc_taxi.staging_trips
FROM '/path/to/yellow_tripdata_2023-01.csv' CSV HEADER;

-- Insert into typed fact table with explicit casts & minimal cleaning
INSERT INTO nyc_taxi.trips (
  vendor_id, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count,
  trip_distance, ratecode_id, store_and_fwd_flag, pulocationid, dolocationid,
  payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount,
  improvement_surcharge, total_amount, congestion_surcharge, airport_fee
)
SELECT
  NULLIF(VendorID,'')::SMALLINT,
  NULLIF(tpep_pickup_datetime,'')::TIMESTAMP,
  NULLIF(tpep_dropoff_datetime,'')::TIMESTAMP,
  NULLIF(passenger_count,'')::SMALLINT,
  NULLIF(trip_distance,'')::NUMERIC(8,3),
  NULLIF(RatecodeID,'')::SMALLINT,
  CASE UPPER(TRIM(store_and_fwd_flag))
    WHEN 'Y' THEN 'Y' WHEN 'N' THEN 'N' ELSE NULL END::CHAR(1),
  NULLIF(PULocationID,'')::INT,
  NULLIF(DOLocationID,'')::INT,
  NULLIF(payment_type,'')::SMALLINT,
  NULLIF(fare_amount,'')::NUMERIC(10,2),
  NULLIF(extra,'')::NUMERIC(10,2),
  NULLIF(mta_tax,'')::NUMERIC(10,2),
  NULLIF(tip_amount,'')::NUMERIC(10,2),
  NULLIF(tolls_amount,'')::NUMERIC(10,2),
  NULLIF(improvement_surcharge,'')::NUMERIC(10,2),
  NULLIF(total_amount,'')::NUMERIC(10,2),
  NULLIF(congestion_surcharge,'')::NUMERIC(10,2),
  NULLIF(airport_fee,'')::NUMERIC(10,2)
FROM nyc_taxi.staging_trips
-- Basic row quality filters (keep them light to avoid throwing away legit rows)
WHERE
  NULLIF(tpep_pickup_datetime,'') IS NOT NULL
  AND NULLIF(tpep_dropoff_datetime,'') IS NOT NULL;

-- ===== Repeat from TRUNCATE … \copy … INSERT for each monthly CSV =====

-- Optional: load multiple months by running the above block per file.
-- Tip: create a psql script that \i includes per-file blocks, or use a shell loop.

-- Quick row count sanity checks
SELECT COUNT(*) AS trip_rows FROM nyc_taxi.trips;
SELECT COUNT(*) AS zones_rows FROM nyc_taxi.zones;

-- Basic validity spot-checks
-- any negative distances?
SELECT COUNT(*) AS negative_distance_rows
FROM nyc_taxi.trips WHERE trip_distance < 0;
-- any NULL pickup timestamps?
SELECT COUNT(*) AS null_pickups
FROM nyc_taxi.trips WHERE tpep_pickup_datetime IS NULL;
