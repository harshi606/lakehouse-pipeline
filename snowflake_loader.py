"""
src/snowflake_loader.py
Snowflake integration: loads transformed data from Parquet/Hive
into Snowflake using the Snowflake Spark Connector.

For local testing without Snowflake credentials, a mock mode
simulates the load and produces analytics SQL output.
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
import logging
import os
import yaml

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def load_config(config_path="config/config.yaml"):
    """Load Snowflake connection config from YAML."""
    if not os.path.exists(config_path):
        logger.warning(f"Config not found at {config_path}. Running in MOCK mode.")
        return None
    with open(config_path) as f:
        return yaml.safe_load(f)


def get_snowflake_options(config: dict) -> dict:
    """Build Snowflake connector options dict."""
    sf = config["snowflake"]
    return {
        "sfURL":       f"{sf['account']}.snowflakecomputing.com",
        "sfUser":      sf["user"],
        "sfPassword":  sf["password"],
        "sfDatabase":  sf["database"],
        "sfSchema":    sf.get("schema", "PUBLIC"),
        "sfWarehouse": sf.get("warehouse", "COMPUTE_WH"),
    }


def write_to_snowflake(df: DataFrame, options: dict, table_name: str):
    """
    Write DataFrame to Snowflake table using Spark connector.
    Requires: net.snowflake:spark-snowflake_2.12 JAR
    """
    logger.info(f"❄️  Writing {df.count():,} records to Snowflake table: {table_name}")
    (
        df.write
        .format("net.snowflake.spark.snowflake")
        .options(**options)
        .option("dbtable", table_name)
        .mode("overwrite")
        .save()
    )
    logger.info(f"   ✅ Snowflake load complete: {table_name}")


def mock_snowflake_analytics(spark: SparkSession, df: DataFrame):
    """
    Mock mode: run Snowflake-equivalent analytics using SparkSQL.
    Produces the same KPI outputs you would see in Snowflake.
    """
    logger.info("\n❄️  [MOCK MODE] Running Snowflake-equivalent analytics via SparkSQL")
    df.createOrReplaceTempView("taxi_trips")

    queries = {
        "1. Revenue by Borough": """
            SELECT
                pickup_borough,
                COUNT(*)                        AS total_trips,
                ROUND(SUM(total_amount), 2)     AS total_revenue,
                ROUND(AVG(fare_amount), 2)      AS avg_fare,
                ROUND(AVG(tip_percentage), 1)   AS avg_tip_pct
            FROM taxi_trips
            GROUP BY pickup_borough
            ORDER BY total_revenue DESC
        """,
        "2. Peak Hour Analysis": """
            SELECT
                pickup_hour,
                time_of_day,
                COUNT(*)                        AS trip_count,
                ROUND(AVG(fare_amount), 2)      AS avg_fare,
                ROUND(AVG(trip_duration_minutes), 1) AS avg_duration_min
            FROM taxi_trips
            GROUP BY pickup_hour, time_of_day
            ORDER BY pickup_hour
        """,
        "3. Weekday vs Weekend KPIs": """
            SELECT
                is_weekend,
                COUNT(*)                        AS total_trips,
                ROUND(AVG(fare_amount), 2)      AS avg_fare,
                ROUND(AVG(trip_distance_miles), 2) AS avg_distance,
                ROUND(AVG(tip_percentage), 1)   AS avg_tip_pct
            FROM taxi_trips
            GROUP BY is_weekend
        """,
        "4. Top Borough Pairs by Volume": """
            SELECT
                pickup_borough,
                dropoff_borough,
                COUNT(*)                        AS trip_count,
                ROUND(AVG(fare_amount), 2)      AS avg_fare
            FROM taxi_trips
            GROUP BY pickup_borough, dropoff_borough
            ORDER BY trip_count DESC
            LIMIT 10
        """,
        "5. Payment Type Distribution": """
            SELECT
                payment_type,
                COUNT(*)                        AS trips,
                ROUND(AVG(tip_amount), 2)       AS avg_tip,
                ROUND(SUM(total_amount), 2)     AS total_revenue
            FROM taxi_trips
            GROUP BY payment_type
            ORDER BY trips DESC
        """
    }

    results = {}
    for name, sql in queries.items():
        print(f"\n{'='*60}")
        print(f"  {name}")
        print('='*60)
        result = spark.sql(sql)
        result.show(truncate=False)
        results[name] = result

    return results


def run_snowflake_load(spark: SparkSession,
                        transformed_path="/tmp/transformed/taxi_clean",
                        config_path="config/config.yaml"):
    """Main Snowflake loading function."""
    logger.info(f"📂 Loading transformed data from: {transformed_path}")
    df = spark.read.parquet(transformed_path)
    logger.info(f"   Records: {df.count():,}")

    config = load_config(config_path)

    if config is None:
        # Mock mode — no Snowflake credentials needed
        return mock_snowflake_analytics(spark, df)
    else:
        options = get_snowflake_options(config)
        write_to_snowflake(df, options, "TAXI_TRIPS_CLEAN")
        logger.info("✅ Data successfully loaded into Snowflake.")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("SnowflakeLoader").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    run_snowflake_load(spark)
