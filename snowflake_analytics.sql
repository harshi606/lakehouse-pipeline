-- ============================================================
-- sql/snowflake_analytics.sql
-- KPI Queries for Snowflake Data Warehouse Layer
-- ============================================================

-- ── Setup ────────────────────────────────────────────────────
CREATE DATABASE IF NOT EXISTS LAKEHOUSE_DB;
CREATE SCHEMA IF NOT EXISTS LAKEHOUSE_DB.TAXI;
USE SCHEMA LAKEHOUSE_DB.TAXI;

-- ── 1. Revenue by Borough ─────────────────────────────────────
SELECT
    PICKUP_BOROUGH,
    COUNT(*)                            AS TOTAL_TRIPS,
    ROUND(SUM(TOTAL_AMOUNT), 2)         AS TOTAL_REVENUE_USD,
    ROUND(AVG(FARE_AMOUNT), 2)          AS AVG_FARE_USD,
    ROUND(AVG(TIP_PERCENTAGE), 1)       AS AVG_TIP_PCT,
    ROUND(AVG(TRIP_DISTANCE_MILES), 2)  AS AVG_DISTANCE_MILES
FROM TAXI_TRIPS_CLEAN
GROUP BY PICKUP_BOROUGH
ORDER BY TOTAL_REVENUE_USD DESC;

-- ── 2. Peak Hour Demand Analysis ─────────────────────────────
SELECT
    PICKUP_HOUR,
    TIME_OF_DAY,
    COUNT(*)                                AS TRIP_COUNT,
    ROUND(AVG(FARE_AMOUNT), 2)              AS AVG_FARE,
    ROUND(AVG(TRIP_DURATION_MINUTES), 1)    AS AVG_DURATION_MIN,
    ROUND(SUM(TOTAL_AMOUNT), 2)             AS TOTAL_REVENUE
FROM TAXI_TRIPS_CLEAN
GROUP BY PICKUP_HOUR, TIME_OF_DAY
ORDER BY PICKUP_HOUR;

-- ── 3. Weekday vs. Weekend KPIs ──────────────────────────────
SELECT
    IS_WEEKEND,
    CASE WHEN IS_WEEKEND THEN 'Weekend' ELSE 'Weekday' END  AS DAY_TYPE,
    COUNT(*)                            AS TOTAL_TRIPS,
    ROUND(AVG(FARE_AMOUNT), 2)          AS AVG_FARE,
    ROUND(AVG(TRIP_DISTANCE_MILES), 2)  AS AVG_DISTANCE,
    ROUND(AVG(TIP_PERCENTAGE), 1)       AS AVG_TIP_PCT,
    ROUND(SUM(TOTAL_AMOUNT), 2)         AS TOTAL_REVENUE
FROM TAXI_TRIPS_CLEAN
GROUP BY IS_WEEKEND
ORDER BY IS_WEEKEND;

-- ── 4. Top Origin-Destination Borough Pairs ──────────────────
SELECT
    PICKUP_BOROUGH,
    DROPOFF_BOROUGH,
    COUNT(*)                    AS TRIP_COUNT,
    ROUND(AVG(FARE_AMOUNT), 2)  AS AVG_FARE,
    ROUND(AVG(TIP_PERCENTAGE),1) AS AVG_TIP_PCT
FROM TAXI_TRIPS_CLEAN
GROUP BY PICKUP_BOROUGH, DROPOFF_BOROUGH
ORDER BY TRIP_COUNT DESC
LIMIT 15;

-- ── 5. Payment Type Revenue Breakdown ────────────────────────
SELECT
    PAYMENT_TYPE,
    COUNT(*)                        AS TRIP_COUNT,
    ROUND(SUM(TOTAL_AMOUNT), 2)     AS TOTAL_REVENUE,
    ROUND(AVG(TIP_AMOUNT), 2)       AS AVG_TIP,
    ROUND(AVG(TIP_PERCENTAGE), 1)   AS AVG_TIP_PCT
FROM TAXI_TRIPS_CLEAN
GROUP BY PAYMENT_TYPE
ORDER BY TRIP_COUNT DESC;

-- ── 6. Fare Anomaly Detection (Above Borough Avg) ────────────
SELECT
    TRIP_ID,
    PICKUP_BOROUGH,
    FARE_AMOUNT,
    BOROUGH_AVG_FARE,
    FARE_VS_BOROUGH_AVG,
    TRIP_DISTANCE_MILES,
    TRIP_DURATION_MINUTES
FROM TAXI_TRIPS_CLEAN
WHERE FARE_VS_BOROUGH_AVG > (BOROUGH_AVG_FARE * 0.5)   -- 50% above borough avg
ORDER BY FARE_VS_BOROUGH_AVG DESC
LIMIT 20;

-- ── 7. Monthly Revenue Trend ─────────────────────────────────
SELECT
    TRIP_YEAR,
    TRIP_MONTH,
    TO_CHAR(DATE_FROM_PARTS(TRIP_YEAR, TRIP_MONTH, 1), 'Mon YYYY') AS MONTH_LABEL,
    COUNT(*)                        AS TOTAL_TRIPS,
    ROUND(SUM(TOTAL_AMOUNT), 2)     AS MONTHLY_REVENUE,
    ROUND(AVG(FARE_AMOUNT), 2)      AS AVG_FARE
FROM TAXI_TRIPS_CLEAN
GROUP BY TRIP_YEAR, TRIP_MONTH
ORDER BY TRIP_YEAR, TRIP_MONTH;

-- ── 8. Speed Outlier Detection ───────────────────────────────
SELECT
    TRIP_ID,
    AVG_SPEED_MPH,
    TRIP_DISTANCE_MILES,
    TRIP_DURATION_MINUTES,
    PICKUP_BOROUGH,
    TIME_OF_DAY
FROM TAXI_TRIPS_CLEAN
WHERE AVG_SPEED_MPH > 60    -- flagging unrealistically fast trips
   OR AVG_SPEED_MPH < 1     -- flagging suspiciously slow trips
ORDER BY AVG_SPEED_MPH DESC;
