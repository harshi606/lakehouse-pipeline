"""
src/transform.py
PySpark transformation layer: feature engineering, outlier removal,
partitioned output ready for Hive registration and Snowflake loading.
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def cast_timestamps(df: DataFrame) -> DataFrame:
    """Cast string datetime columns to proper TimestampType."""
    return (
        df
        .withColumn("pickup_datetime",  F.to_timestamp("pickup_datetime",  "yyyy-MM-dd HH:mm:ss"))
        .withColumn("dropoff_datetime", F.to_timestamp("dropoff_datetime", "yyyy-MM-dd HH:mm:ss"))
    )


def engineer_features(df: DataFrame) -> DataFrame:
    """
    Feature engineering:
    - trip_duration_minutes
    - avg_speed_mph
    - fare_per_mile
    - tip_percentage
    - time_of_day bucket (Morning/Afternoon/Evening/Night)
    - is_weekend flag
    """
    logger.info("⚙️  Engineering features...")

    df = cast_timestamps(df)

    df = (
        df
        # Trip duration in minutes
        .withColumn(
            "trip_duration_minutes",
            F.round(
                (F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime")) / 60,
                2
            )
        )
        # Average speed
        .withColumn(
            "avg_speed_mph",
            F.round(
                F.col("trip_distance_miles") / (F.col("trip_duration_minutes") / 60),
                2
            )
        )
        # Fare efficiency
        .withColumn(
            "fare_per_mile",
            F.round(F.col("fare_amount") / F.col("trip_distance_miles"), 2)
        )
        # Tip percentage
        .withColumn(
            "tip_percentage",
            F.round((F.col("tip_amount") / F.col("fare_amount")) * 100, 1)
        )
        # Hour of pickup
        .withColumn("pickup_hour", F.hour("pickup_datetime"))
        # Time of day bucket
        .withColumn(
            "time_of_day",
            F.when(F.col("pickup_hour").between(6, 11),  "Morning")
             .when(F.col("pickup_hour").between(12, 16), "Afternoon")
             .when(F.col("pickup_hour").between(17, 21), "Evening")
             .otherwise("Night")
        )
        # Weekend flag
        .withColumn(
            "is_weekend",
            F.when(F.dayofweek("pickup_datetime").isin([1, 7]), True).otherwise(False)
        )
        # Day of week name
        .withColumn(
            "day_of_week",
            F.date_format("pickup_datetime", "EEEE")
        )
    )

    logger.info("   ✅ Feature engineering complete.")
    return df


def remove_outliers(df: DataFrame) -> DataFrame:
    """
    Remove statistical outliers using IQR method on key numeric columns.
    Keeps records within 1.5x IQR of Q1/Q3.
    """
    logger.info("🔍 Removing outliers...")

    numeric_cols = ["trip_distance_miles", "fare_amount", "trip_duration_minutes", "avg_speed_mph"]

    for col in numeric_cols:
        quantiles = df.approxQuantile(col, [0.25, 0.75], 0.01)
        if len(quantiles) == 2:
            q1, q3 = quantiles
            iqr = q3 - q1
            lower = q1 - 1.5 * iqr
            upper = q3 + 1.5 * iqr
            before = df.count()
            df = df.filter(F.col(col).between(lower, upper))
            after = df.count()
            logger.info(f"   {col}: removed {before - after:,} outliers (range: {lower:.2f} to {upper:.2f})")

    return df


def add_borough_metrics(df: DataFrame) -> DataFrame:
    """
    Add borough-level aggregated metrics as window functions:
    - borough_avg_fare: avg fare for that pickup borough
    - borough_trip_rank: rank of fare within borough
    """
    logger.info("📍 Computing borough-level metrics...")

    window_borough = Window.partitionBy("pickup_borough")

    df = (
        df
        .withColumn("borough_avg_fare",    F.round(F.avg("fare_amount").over(window_borough), 2))
        .withColumn("borough_avg_distance", F.round(F.avg("trip_distance_miles").over(window_borough), 2))
        .withColumn(
            "fare_vs_borough_avg",
            F.round(F.col("fare_amount") - F.col("borough_avg_fare"), 2)
        )
    )

    return df


def select_final_columns(df: DataFrame) -> DataFrame:
    """Select and order final columns for Hive/Snowflake output."""
    return df.select(
        "trip_id",
        "pickup_datetime",
        "dropoff_datetime",
        "pickup_borough",
        "dropoff_borough",
        "passenger_count",
        "trip_distance_miles",
        "trip_duration_minutes",
        "avg_speed_mph",
        "fare_amount",
        "tip_amount",
        "tip_percentage",
        "total_amount",
        "fare_per_mile",
        "payment_type",
        "pickup_hour",
        "time_of_day",
        "day_of_week",
        "is_weekend",
        "borough_avg_fare",
        "borough_avg_distance",
        "fare_vs_borough_avg",
        "trip_year",
        "trip_month"
    )


def run_transformation(spark: SparkSession,
                       staging_path="/tmp/staging/taxi_raw",
                       output_path="/tmp/transformed/taxi_clean"):
    """Full transformation pipeline."""
    logger.info(f"\n🔄 Starting transformation from: {staging_path}")

    df = spark.read.parquet(staging_path)
    logger.info(f"   Records loaded: {df.count():,}")

    df = engineer_features(df)
    df = remove_outliers(df)
    df = add_borough_metrics(df)
    df = select_final_columns(df)

    logger.info(f"💾 Writing transformed data to: {output_path}")
    (
        df.write
        .mode("overwrite")
        .partitionBy("trip_year", "trip_month")
        .parquet(output_path)
    )

    logger.info(f"✅ Transformation complete. Final records: {df.count():,}")
    return df


if __name__ == "__main__":
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName("Transform").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df = run_transformation(spark)
    print("\n📊 Sample transformed data:")
    df.select(
        "trip_id", "trip_distance_miles", "trip_duration_minutes",
        "avg_speed_mph", "fare_per_mile", "time_of_day", "borough_avg_fare"
    ).show(10, truncate=False)

    print("\n📈 Borough Summary:")
    df.groupBy("pickup_borough").agg(
        F.count("trip_id").alias("total_trips"),
        F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
        F.round(F.avg("trip_distance_miles"), 2).alias("avg_distance"),
        F.round(F.avg("tip_percentage"), 1).alias("avg_tip_pct")
    ).orderBy("total_trips", ascending=False).show()
