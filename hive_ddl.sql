-- ============================================================
-- sql/hive_ddl.sql
-- Hive Metastore DDL for NYC Taxi Lakehouse Pipeline
-- ============================================================

-- Create database
CREATE DATABASE IF NOT EXISTS nyc_lakehouse
COMMENT 'NYC Taxi Lakehouse Data Warehouse'
LOCATION '/user/hive/warehouse/nyc_lakehouse.db';

-- Drop and recreate table (idempotent)
DROP TABLE IF EXISTS nyc_lakehouse.taxi_trips;

-- Create external partitioned table over Parquet files
CREATE EXTERNAL TABLE nyc_lakehouse.taxi_trips (
    trip_id                 STRING          COMMENT 'Unique trip identifier',
    pickup_datetime         TIMESTAMP       COMMENT 'Trip pickup timestamp',
    dropoff_datetime        TIMESTAMP       COMMENT 'Trip dropoff timestamp',
    pickup_borough          STRING          COMMENT 'NYC borough of pickup',
    dropoff_borough         STRING          COMMENT 'NYC borough of dropoff',
    passenger_count         INT             COMMENT 'Number of passengers',
    trip_distance_miles     DOUBLE          COMMENT 'Trip distance in miles',
    trip_duration_minutes   DOUBLE          COMMENT 'Computed trip duration in minutes',
    avg_speed_mph           DOUBLE          COMMENT 'Average speed miles per hour',
    fare_amount             DOUBLE          COMMENT 'Base fare in USD',
    tip_amount              DOUBLE          COMMENT 'Tip amount in USD',
    tip_percentage          DOUBLE          COMMENT 'Tip as percentage of fare',
    total_amount            DOUBLE          COMMENT 'Total charge including tip and fees',
    fare_per_mile           DOUBLE          COMMENT 'Efficiency metric: fare divided by distance',
    payment_type            STRING          COMMENT 'Payment method',
    pickup_hour             INT             COMMENT 'Hour of pickup (0-23)',
    time_of_day             STRING          COMMENT 'Morning/Afternoon/Evening/Night bucket',
    day_of_week             STRING          COMMENT 'Day name of pickup',
    is_weekend              BOOLEAN         COMMENT 'True if pickup was on Saturday or Sunday',
    borough_avg_fare        DOUBLE          COMMENT 'Window avg fare for pickup borough',
    borough_avg_distance    DOUBLE          COMMENT 'Window avg distance for pickup borough',
    fare_vs_borough_avg     DOUBLE          COMMENT 'Fare deviation from borough average'
)
PARTITIONED BY (
    trip_year   INT     COMMENT 'Partition: year of trip',
    trip_month  INT     COMMENT 'Partition: month of trip'
)
STORED AS PARQUET
LOCATION 'hdfs:///data/nyc_lakehouse/taxi_trips/'
TBLPROPERTIES (
    'parquet.compression'   = 'SNAPPY',
    'creator'               = 'lakehouse_pipeline',
    'created_at'            = '2024-01-01'
);

-- Discover and register all partitions automatically
MSCK REPAIR TABLE nyc_lakehouse.taxi_trips;

-- Verify partition count
SHOW PARTITIONS nyc_lakehouse.taxi_trips;

-- ============================================================
-- Useful Hive queries post-registration
-- ============================================================

-- 1. Partition pruning example (efficient scan)
SELECT COUNT(*) AS jan_trips
FROM nyc_lakehouse.taxi_trips
WHERE trip_year = 2023
  AND trip_month = 1;

-- 2. Borough revenue summary
SELECT
    pickup_borough,
    COUNT(*)                        AS total_trips,
    ROUND(SUM(total_amount), 2)     AS total_revenue,
    ROUND(AVG(fare_amount), 2)      AS avg_fare
FROM nyc_lakehouse.taxi_trips
GROUP BY pickup_borough
ORDER BY total_revenue DESC;

-- 3. Monthly trend
SELECT
    trip_year,
    trip_month,
    COUNT(*)                        AS trips,
    ROUND(AVG(fare_amount), 2)      AS avg_fare
FROM nyc_lakehouse.taxi_trips
GROUP BY trip_year, trip_month
ORDER BY trip_year, trip_month;
