"""
main.py
Orchestrates the full NYC Taxi Data Lakehouse Pipeline:
  1. Generate sample data
  2. Ingest + validate (PySpark)
  3. Transform + feature engineer (PySpark)
  4. Register Hive Metastore table
  5. Load to Snowflake (or mock analytics)
"""
import time
import logging
from pyspark.sql import SparkSession

from src.ingest import run_ingestion
from src.transform import run_transformation
from src.hive_register import register_hive_table, verify_hive_table
from src.snowflake_loader import run_snowflake_load

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger(__name__)

# ── Config ───────────────────────────────────────────────────────
RAW_INPUT       = "data/sample_taxi.csv"
STAGING_PATH    = "/tmp/staging/taxi_raw"
TRANSFORMED_PATH= "/tmp/transformed/taxi_clean"
CONFIG_PATH     = "config/config.yaml"


def build_spark():
    return (
        SparkSession.builder
        .appName("NYC_Taxi_Lakehouse")
        .config("spark.sql.warehouse.dir", "/tmp/hive_warehouse")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def main():
    total_start = time.time()
    print("\n" + "="*65)
    print("  🏗️  NYC TAXI DATA LAKEHOUSE PIPELINE")
    print("  PySpark  |  Hive Metastore  |  Snowflake")
    print("="*65 + "\n")

    # ── Step 0: Generate sample data ─────────────────────────
    import os
    if not os.path.exists(RAW_INPUT):
        logger.info("Step 0: Generating sample data...")
        import sys
        sys.path.insert(0, "data")
        from generate_sample_data import generate_sample
        generate_sample(n=5000, output_path=RAW_INPUT)
    else:
        logger.info("Step 0: Sample data already exists, skipping generation.")

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    # ── Step 1: Ingest ────────────────────────────────────────
    t1 = time.time()
    logger.info("\n" + "─"*50)
    logger.info("Step 1: Ingestion + Data Quality Validation")
    logger.info("─"*50)
    _, clean_df, quality_report = run_ingestion(RAW_INPUT, STAGING_PATH)
    logger.info(f"   Done in {time.time()-t1:.1f}s | Clean records: {quality_report['clean_records']:,}")

    # ── Step 2: Transform ─────────────────────────────────────
    t2 = time.time()
    logger.info("\n" + "─"*50)
    logger.info("Step 2: PySpark Transformation + Feature Engineering")
    logger.info("─"*50)
    transformed_df = run_transformation(spark, STAGING_PATH, TRANSFORMED_PATH)
    logger.info(f"   Done in {time.time()-t2:.1f}s | Final records: {transformed_df.count():,}")

    # ── Step 3: Hive Registration ─────────────────────────────
    t3 = time.time()
    logger.info("\n" + "─"*50)
    logger.info("Step 3: Hive Metastore Registration")
    logger.info("─"*50)
    register_hive_table(spark, TRANSFORMED_PATH)
    verify_hive_table(spark)
    logger.info(f"   Done in {time.time()-t3:.1f}s")

    # ── Step 4: Snowflake Load / Analytics ───────────────────
    t4 = time.time()
    logger.info("\n" + "─"*50)
    logger.info("Step 4: Snowflake Load + KPI Analytics")
    logger.info("─"*50)
    run_snowflake_load(spark, TRANSFORMED_PATH, CONFIG_PATH)
    logger.info(f"   Done in {time.time()-t4:.1f}s")

    # ── Summary ───────────────────────────────────────────────
    total_elapsed = time.time() - total_start
    print("\n" + "="*65)
    print("  ✅ PIPELINE COMPLETE")
    print(f"  Total time       : {total_elapsed:.1f}s")
    print(f"  Records ingested : {quality_report['clean_records']:,}")
    print(f"  Records rejected : {quality_report['rejected_records']:,}")
    print(f"  Staging path     : {STAGING_PATH}")
    print(f"  Transformed path : {TRANSFORMED_PATH}")
    print(f"  Hive table       : nyc_lakehouse.taxi_trips")
    print("="*65 + "\n")

    spark.stop()


if __name__ == "__main__":
    main()
