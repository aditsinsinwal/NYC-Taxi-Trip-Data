-- Converts nyc_taxi.trips into a monthly RANGE-partitioned table (by pickup timestamp)
-- and creates all needed monthly partitions based on the data you’ve loaded.

SET search_path = nyc_taxi, public;

-- Safety: create schema if someone skipped step 1
CREATE SCHEMA IF NOT EXISTS nyc_taxi;

-- 0) Detect whether trips is already partitioned
DO $$
DECLARE
  is_part BOOLEAN;
BEGIN
  SELECT EXISTS (
    SELECT 1
    FROM pg_partitioned_table
    WHERE partrelid = 'nyc_taxi.trips'::regclass
  )
  INTO is_part;

  IF is_part THEN
    RAISE NOTICE 'Table nyc_taxi.trips is already partitioned. Creating any missing monthly partitions…';
  ELSE
    RAISE NOTICE 'Converting nyc_taxi.trips to a range-partitioned table…';

    -- 1) Create a partitioned clone with identical structure/constraints
    EXECUTE $sql$
      CREATE TABLE IF NOT EXISTS nyc_taxi.trips_part
      (LIKE nyc_taxi.trips INCLUDING ALL)
      PARTITION BY RANGE (tpep_pickup_datetime)
    $sql$;

  END IF;
END$$;

-- 1.5) Work out the date range to cover (min..max months present in data, or a default small range)
DO $$
DECLARE
  min_ts TIMESTAMP;
  max_ts TIMESTAMP;
  start_month DATE;
  end_month_exclusive DATE;
BEGIN
  -- If trips already partitioned, query from it; else from non-partitioned trips
  IF EXISTS (SELECT 1 FROM pg_class WHERE oid='nyc_taxi.trips'::regclass) THEN
    EXECUTE 'SELECT min(tpep_pickup_datetime), max(tpep_pickup_datetime) FROM nyc_taxi.trips'
      INTO min_ts, max_ts;
  END IF;

  IF min_ts IS NULL OR max_ts IS NULL THEN
    -- no data yet; create a small placeholder window to avoid errors
    start_month := DATE '2023-01-01';
    end_month_exclusive := DATE '2023-03-01';
  ELSE
    start_month := date_trunc('month', min_ts)::date;
    -- make exclusive end = first day of the month AFTER max pickup
    end_month_exclusive := (date_trunc('month', max_ts)::date + INTERVAL '1 month')::date;
  END IF;

  -- 2) Create monthly partitions over [start_month, end_month_exclusive)
  WHILE start_month < end_month_exclusive LOOP
    EXECUTE format(
      'CREATE TABLE IF NOT EXISTS nyc_taxi.trips_p_%s PARTITION OF %I
         FOR VALUES FROM (%L) TO (%L);',
      to_char(start_month,'YYYYMM'),
      CASE
        WHEN EXISTS (SELECT 1 FROM pg_partitioned_table WHERE partrelid='nyc_taxi.trips'::regclass)
          THEN 'nyc_taxi.trips'
        ELSE 'nyc_taxi.trips_part'
      END,
      start_month::text,
      (start_month + INTERVAL '1 month')::date::text
    );
    start_month := (start_month + INTERVAL '1 month')::date;
  END LOOP;
END$$;

-- 3) If we’re migrating, move data and swap tables
DO $$
DECLARE
  is_part BOOLEAN;
BEGIN
  SELECT EXISTS (
    SELECT 1
    FROM pg_partitioned_table
    WHERE partrelid = 'nyc_taxi.trips'::regclass
  ) INTO is_part;

  IF NOT is_part THEN
    RAISE NOTICE 'Moving rows into partitioned table (this can take time)…';
    EXECUTE 'INSERT INTO nyc_taxi.trips_part SELECT * FROM nyc_taxi.trips';

    -- Clean up any leftover legacy table and swap names
    EXECUTE 'DROP TABLE IF EXISTS nyc_taxi.trips_raw CASCADE';
    EXECUTE 'ALTER TABLE nyc_taxi.trips RENAME TO trips_raw';
    EXECUTE 'ALTER TABLE nyc_taxi.trips_part RENAME TO trips';

    -- Ensure the trip_id sequence is set correctly after the copy+swap
    PERFORM setval(
      pg_get_serial_sequence('nyc_taxi.trips','trip_id'),
      (SELECT COALESCE(MAX(trip_id), 1) FROM nyc_taxi.trips),
      true
    );

    RAISE NOTICE 'Swap complete. Original table kept as nyc_taxi.trips_raw.';
  END IF;
END$$;

-- 4) Optional: default partition to catch out-of-range rows
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_class
    WHERE relkind = 'r' AND relname = 'trips_p_default'
          AND relnamespace = 'nyc_taxi'::regnamespace
  ) THEN
    EXECUTE 'CREATE TABLE nyc_taxi.trips_p_default PARTITION OF nyc_taxi.trips DEFAULT';
  END IF;
END$$;

-- 5) Recreate critical parent indexes (partitioned indexes propagate to children)
--    It’s safe to re-run; IF NOT EXISTS guards included.
CREATE INDEX IF NOT EXISTS idx_trips_pickup_ts_brin
  ON nyc_taxi.trips USING BRIN (tpep_pickup_datetime) WITH (pages_per_range = 128);

CREATE INDEX IF NOT EXISTS idx_trips_pickup_ts_btree
  ON nyc_taxi.trips (tpep_pickup_datetime);

CREATE INDEX IF NOT EXISTS idx_trips_route_revenue
  ON nyc_taxi.trips (pulocationid, dolocationid) INCLUDE (total_amount);

CREATE INDEX IF NOT EXISTS idx_trips_payment_tip
  ON nyc_taxi.trips (payment_type) INCLUDE (fare_amount, tip_amount);

-- 6) Fresh stats for good plans
ANALYZE nyc_taxi.trips;
