"""
src/hive_register.py
Hive Metastore integration: creates external partitioned tables
over the transformed Parquet data. Uses SparkSQL with Hive support.
"""
from pyspark.sql import SparkSession
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── DDL Templates ────────────────────────────────────────────────

CREATE_DATABASE_SQL = "CREATE DATABASE IF NOT EXISTS nyc_lakehouse"

DROP_TABLE_SQL = "DROP TABLE IF EXISTS nyc_lakehouse.taxi_trips"

CREATE_TABLE_SQL = """
CREATE EXTERNAL TABLE IF NOT EXISTS nyc_lakehouse.taxi_trips (
    trip_id                 STRING,
    pickup_datetime         TIMESTAMP,
    dropoff_datetime        TIMESTAMP,
    pickup_borough          STRING,
    dropoff_borough         STRING,
    passenger_count         INT,
    trip_distance_miles     DOUBLE,
    trip_duration_minutes   DOUBLE,
    avg_speed_mph           DOUBLE,
    fare_amount             DOUBLE,
    tip_amount              DOUBLE,
    tip_percentage          DOUBLE,
    total_amount            DOUBLE,
    fare_per_mile           DOUBLE,
    payment_type            STRING,
    pickup_hour             INT,
    time_of_day             STRING,
    day_of_week             STRING,
    is_weekend              BOOLEAN,
    borough_avg_fare        DOUBLE,
    borough_avg_distance    DOUBLE,
    fare_vs_borough_avg     DOUBLE
)
PARTITIONED BY (
    trip_year   INT,
    trip_month  INT
)
STORED AS PARQUET
LOCATION '{location}'
TBLPROPERTIES ('parquet.compression'='SNAPPY')
"""

REPAIR_TABLE_SQL = "MSCK REPAIR TABLE nyc_lakehouse.taxi_trips"

VERIFY_SQL = """
SELECT
    trip_year,
    trip_month,
    COUNT(*)        AS total_trips,
    ROUND(AVG(fare_amount), 2)  AS avg_fare,
    ROUND(AVG(trip_distance_miles), 2) AS avg_distance
FROM nyc_lakehouse.taxi_trips
GROUP BY trip_year, trip_month
ORDER BY trip_year, trip_month
"""


def register_hive_table(spark: SparkSession,
                         parquet_path="/tmp/transformed/taxi_clean"):
    """
    Register the transformed Parquet dataset as a Hive external table.
    Partitions are auto-discovered via MSCK REPAIR TABLE.
    """
    logger.info("🐝 Registering Hive Metastore table...")

    spark.sql(CREATE_DATABASE_SQL)
    logger.info("   Database: nyc_lakehouse ✅")

    spark.sql(DROP_TABLE_SQL)

    create_sql = CREATE_TABLE_SQL.format(location=parquet_path)
    spark.sql(create_sql)
    logger.info("   Table: nyc_lakehouse.taxi_trips ✅")

    # Discover partitions automatically
    try:
        spark.sql(REPAIR_TABLE_SQL)
        logger.info("   Partitions repaired ✅")
    except Exception as e:
        logger.warning(f"   MSCK REPAIR skipped (in-memory catalog): {e}")
        # For in-memory catalog: register partitions manually from parquet
        df = spark.read.parquet(parquet_path)
        df.write.mode("overwrite").saveAsTable("nyc_lakehouse.taxi_trips")
        logger.info("   Table registered via saveAsTable ✅")

    logger.info("✅ Hive registration complete.")


def verify_hive_table(spark: SparkSession):
    """Run verification query to confirm data is accessible via Hive."""
    logger.info("\n🔍 Verifying Hive table contents...")
    try:
        result = spark.sql(VERIFY_SQL)
        result.show(truncate=False)
        return result
    except Exception as e:
        logger.error(f"Verification failed: {e}")

        # Fallback: query the registered table directly
        try:
            spark.sql("SELECT COUNT(*) as total FROM nyc_lakehouse.taxi_trips").show()
        except Exception:
            pass


def show_partition_info(spark: SparkSession):
    """Show partition metadata."""
    try:
        spark.sql("SHOW PARTITIONS nyc_lakehouse.taxi_trips").show()
    except Exception as e:
        logger.warning(f"Partition info not available: {e}")


if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("HiveRegister")
        .config("spark.sql.warehouse.dir", "/tmp/hive_warehouse")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    register_hive_table(spark)
    verify_hive_table(spark)
    show_partition_info(spark)
