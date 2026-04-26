"""
src/ingest.py
PySpark ingestion layer: reads raw CSV, enforces schema,
validates data quality, and writes clean Parquet to staging.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, TimestampType
)
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── Schema Definition ────────────────────────────────────────────
RAW_SCHEMA = StructType([
    StructField("trip_id",             StringType(),    True),
    StructField("pickup_datetime",     StringType(),    True),
    StructField("dropoff_datetime",    StringType(),    True),
    StructField("pickup_borough",      StringType(),    True),
    StructField("dropoff_borough",     StringType(),    True),
    StructField("passenger_count",     IntegerType(),   True),
    StructField("trip_distance_miles", DoubleType(),    True),
    StructField("fare_amount",         DoubleType(),    True),
    StructField("tip_amount",          DoubleType(),    True),
    StructField("total_amount",        DoubleType(),    True),
    StructField("payment_type",        StringType(),    True),
    StructField("trip_year",           IntegerType(),   True),
    StructField("trip_month",          IntegerType(),   True),
])


def create_spark_session(app_name="NYC_Taxi_Lakehouse"):
    """Initialize SparkSession with Hive support."""
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.warehouse.dir", "/tmp/hive_warehouse")
        .config("spark.sql.catalogImplementation", "in-memory")   # swap to 'hive' with metastore
        .config("spark.sql.parquet.compression.codec", "snappy")
        .enableHiveSupport() if False else   # set True when Hive metastore is available
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.warehouse.dir", "/tmp/hive_warehouse")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def ingest_raw(spark, input_path="data/sample_taxi.csv"):
    """
    Read raw CSV with enforced schema.
    Returns raw DataFrame.
    """
    logger.info(f"📥 Ingesting raw data from: {input_path}")
    df = spark.read.csv(
        input_path,
        schema=RAW_SCHEMA,
        header=True,
        timestampFormat="yyyy-MM-dd HH:mm:ss"
    )
    logger.info(f"   Rows ingested: {df.count():,}")
    return df


def validate_quality(df):
    """
    Data quality checks:
    - Null checks on critical columns
    - Range validation (fare > 0, distance > 0)
    - Duplicate trip_id detection
    Returns (clean_df, quality_report dict)
    """
    total = df.count()

    # Null counts
    null_counts = {
        col: df.filter(F.col(col).isNull()).count()
        for col in ["trip_id", "pickup_datetime", "fare_amount", "trip_distance_miles"]
    }

    # Invalid ranges
    invalid_fare    = df.filter(F.col("fare_amount") <= 0).count()
    invalid_dist    = df.filter(F.col("trip_distance_miles") <= 0).count()
    duplicate_ids   = total - df.dropDuplicates(["trip_id"]).count()

    quality_report = {
        "total_records":    total,
        "null_counts":      null_counts,
        "invalid_fare":     invalid_fare,
        "invalid_distance": invalid_dist,
        "duplicate_ids":    duplicate_ids,
    }

    # Filter to clean records only
    clean_df = (
        df
        .dropDuplicates(["trip_id"])
        .filter(F.col("fare_amount") > 0)
        .filter(F.col("trip_distance_miles") > 0)
        .filter(F.col("trip_id").isNotNull())
    )

    clean_count = clean_df.count()
    quality_report["clean_records"] = clean_count
    quality_report["rejected_records"] = total - clean_count

    logger.info("📊 Data Quality Report:")
    for k, v in quality_report.items():
        logger.info(f"   {k}: {v}")

    return clean_df, quality_report


def write_staging(df, output_path="/tmp/staging/taxi_raw"):
    """Write clean data to Parquet staging area, partitioned by year/month."""
    logger.info(f"💾 Writing staging Parquet to: {output_path}")
    (
        df.write
        .mode("overwrite")
        .partitionBy("trip_year", "trip_month")
        .parquet(output_path)
    )
    logger.info("   ✅ Staging write complete.")


def run_ingestion(input_path="data/sample_taxi.csv",
                  staging_path="/tmp/staging/taxi_raw"):
    spark = create_spark_session()
    raw_df = ingest_raw(spark, input_path)
    clean_df, report = validate_quality(raw_df)
    write_staging(clean_df, staging_path)
    return spark, clean_df, report


if __name__ == "__main__":
    spark, df, report = run_ingestion()
    print("\n✅ Ingestion complete.")
    print(f"   Clean records ready: {report['clean_records']:,}")
    df.printSchema()
    df.show(5, truncate=False)
