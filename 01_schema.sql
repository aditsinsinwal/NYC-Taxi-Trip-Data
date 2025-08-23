-- Schema
CREATE SCHEMA IF NOT EXISTS nyc_taxi;
SET search_path = nyc_taxi, public;

-- TLC taxi zones from taxi_zone_lookup.csv
CREATE TABLE IF NOT EXISTS zones (
  location_id    INT PRIMARY KEY,
  borough        TEXT NOT NULL,
  zone           TEXT NOT NULL,
  service_zone   TEXT NOT NULL
);

-- payment types (per TLC spec)
CREATE TABLE IF NOT EXISTS payment_types (
  payment_type   SMALLINT PRIMARY KEY,
  description    TEXT NOT NULL
);

INSERT INTO payment_types (payment_type, description) VALUES
  (1,'Credit card'), (2,'Cash'), (3,'No charge'),
  (4,'Dispute'), (5,'Unknown'), (6,'Voided trip')
ON CONFLICT DO NOTHING;

-- Core fact: trips (NYC Yellow Taxi)
CREATE TABLE IF NOT EXISTS trips (
  trip_id                 BIGSERIAL PRIMARY KEY,
  vendor_id               SMALLINT,
  tpep_pickup_datetime    TIMESTAMP NOT NULL,
  tpep_dropoff_datetime   TIMESTAMP NOT NULL,
  passenger_count         SMALLINT,
  trip_distance           NUMERIC(8,3) CHECK (trip_distance >= 0),
  ratecode_id             SMALLINT,
  store_and_fwd_flag      CHAR(1) CHECK (store_and_fwd_flag IN ('Y','N')),
  pulocationid            INT REFERENCES zones(location_id),
  dolocationid            INT REFERENCES zones(location_id),
  payment_type            SMALLINT REFERENCES payment_types(payment_type),
  fare_amount             NUMERIC(10,2),
  extra                   NUMERIC(10,2),
  mta_tax                 NUMERIC(10,2),
  tip_amount              NUMERIC(10,2),
  tolls_amount            NUMERIC(10,2),
  improvement_surcharge   NUMERIC(10,2),
  congestion_surcharge    NUMERIC(10,2),
  airport_fee             NUMERIC(10,2),
  total_amount            NUMERIC(10,2),
  -- sanity checks (keep flexible: total can be negative on adjustments)
  CHECK (passenger_count IS NULL OR passenger_count >= 0),
  CHECK (fare_amount IS NULL OR fare_amount >= -2000),
  CHECK (total_amount IS NULL OR total_amount >= -2000)
);

-- Staging table for raw CSV loads (all text for fast COPY, cast later)
CREATE TABLE IF NOT EXISTS staging_trips (
  VendorID               TEXT,
  tpep_pickup_datetime   TEXT,
  tpep_dropoff_datetime  TEXT,
  passenger_count        TEXT,
  trip_distance          TEXT,
  RatecodeID             TEXT,
  store_and_fwd_flag     TEXT,
  PULocationID           TEXT,
  DOLocationID           TEXT,
  payment_type           TEXT,
  fare_amount            TEXT,
  extra                  TEXT,
  mta_tax                TEXT,
  tip_amount             TEXT,
  tolls_amount           TEXT,
  improvement_surcharge  TEXT,
  total_amount           TEXT,
  congestion_surcharge   TEXT,
  airport_fee            TEXT
);
