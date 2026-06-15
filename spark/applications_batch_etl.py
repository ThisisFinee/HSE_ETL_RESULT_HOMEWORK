import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, countDistinct, round as spark_round, sum as spark_sum, to_date, to_timestamp, when
from pyspark.sql.types import BooleanType, IntegerType


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Batch ETL for loan application files.")
    parser.add_argument("--input", required=True, help="Input CSV or Parquet path.")
    parser.add_argument("--output", required=True, help="Output directory for analytical marts.")
    parser.add_argument("--format", choices=["csv", "parquet"], default="csv", help="Input format.")
    return parser.parse_args()


def read_source(spark: SparkSession, input_path: str, input_format: str):
    if input_format == "parquet":
        return spark.read.parquet(input_path)

    return (
        spark.read.option("header", True)
        .option("multiLine", False)
        .option("escape", '"')
        .csv(input_path)
    )


def normalize_applications(df):
    return (
        df.select(
            col("application_id"),
            to_timestamp("event_time", "yyyy-MM-dd HH:mm:ss").alias("event_time"),
            col("customer_id"),
            col("region_code"),
            col("product_type"),
            col("requested_amount").cast(IntegerType()).alias("requested_amount"),
            col("term_months").cast(IntegerType()).alias("term_months"),
            col("credit_score").cast(IntegerType()).alias("credit_score"),
            col("risk_level"),
            col("decision_status"),
            col("approved_amount").cast(IntegerType()).alias("approved_amount"),
            col("channel"),
            col("employee_review_flag").cast(BooleanType()).alias("employee_review_flag"),
            col("processing_time_sec").cast(IntegerType()).alias("processing_time_sec"),
        )
        .filter(col("application_id").isNotNull())
        .filter(col("event_time").isNotNull())
        .filter(col("requested_amount") > 0)
        .filter(col("credit_score").between(300, 900))
    )


def write_parquet(df, path: str) -> None:
    df.coalesce(1).write.mode("overwrite").parquet(path)


def main() -> None:
    args = parse_args()
    spark = (
        SparkSession.builder.appName("applications-batch-etl")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    source = read_source(spark, args.input, args.format)
    applications = normalize_applications(source).cache()

    applications_by_region_status = (
        applications.groupBy("region_code", "decision_status")
        .agg(
            count("*").alias("application_count"),
            countDistinct("customer_id").alias("customer_count"),
            spark_round(avg("requested_amount"), 2).alias("avg_requested_amount"),
        )
        .orderBy("region_code", "decision_status")
    )

    risk_level_metrics = (
        applications.groupBy("risk_level")
        .agg(
            count("*").alias("application_count"),
            spark_round(avg("credit_score"), 2).alias("avg_credit_score"),
            spark_round(avg("requested_amount"), 2).alias("avg_requested_amount"),
            spark_round(avg("approved_amount"), 2).alias("avg_approved_amount"),
        )
        .orderBy("risk_level")
    )

    manual_review_by_channel = (
        applications.groupBy("channel")
        .agg(
            count("*").alias("application_count"),
            spark_sum(when(col("employee_review_flag"), 1).otherwise(0)).alias("manual_review_count"),
            spark_round(avg(col("employee_review_flag").cast("int")) * 100, 2).alias("manual_review_pct"),
        )
        .orderBy("channel")
    )

    daily_processing_metrics = (
        applications.withColumn("event_date", to_date("event_time"))
        .groupBy("event_date")
        .agg(
            count("*").alias("application_count"),
            spark_round(avg("processing_time_sec"), 2).alias("avg_processing_time_sec"),
            spark_round(avg("credit_score"), 2).alias("avg_credit_score"),
        )
        .orderBy("event_date")
    )

    write_parquet(applications, f"{args.output.rstrip('/')}/clean_applications")
    write_parquet(applications_by_region_status, f"{args.output.rstrip('/')}/applications_by_region_status")
    write_parquet(risk_level_metrics, f"{args.output.rstrip('/')}/risk_level_metrics")
    write_parquet(manual_review_by_channel, f"{args.output.rstrip('/')}/manual_review_by_channel")
    write_parquet(daily_processing_metrics, f"{args.output.rstrip('/')}/daily_processing_metrics")

    spark.stop()


if __name__ == "__main__":
    main()
