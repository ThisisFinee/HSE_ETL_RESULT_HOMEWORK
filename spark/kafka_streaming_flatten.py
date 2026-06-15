import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode_outer, from_json, to_timestamp
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


APPLICATION_SCHEMA = StructType(
    [
        StructField("application_id", StringType(), False),
        StructField(
            "customer",
            StructType(
                [
                    StructField("customer_id", StringType(), True),
                    StructField("region", StringType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "loan",
            StructType(
                [
                    StructField("amount", IntegerType(), True),
                    StructField("term_months", IntegerType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "scoring",
            StructType(
                [
                    StructField("score", IntegerType(), True),
                    StructField("risk_level", StringType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "documents",
            ArrayType(
                StructType(
                    [
                        StructField("type", StringType(), True),
                        StructField("status", StringType(), True),
                    ]
                )
            ),
            True,
        ),
        StructField("decision_status", StringType(), True),
        StructField("submitted_at", StringType(), True),
    ]
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Flatten Kafka loan application JSON with PySpark Structured Streaming.")
    parser.add_argument("--bootstrap-servers", default=None, help="Kafka bootstrap servers.")
    parser.add_argument("--topic", default=None, help="Kafka topic to consume.")
    parser.add_argument("--output", default=None, help="Output path for flattened Parquet files.")
    parser.add_argument("--checkpoint", default=None, help="Checkpoint path.")
    parser.add_argument("--starting-offsets", default="earliest", choices=["earliest", "latest"])
    parser.add_argument("--run-mode", default="once", choices=["once", "continuous"])
    parser.add_argument("--timeout-seconds", type=int, default=None)
    parser.add_argument("--kafka-security-protocol", default=None)
    parser.add_argument("--kafka-sasl-mechanism", default=None)
    parser.add_argument("--kafka-sasl-jaas-config", default=None)
    return parser.parse_args()


def resolve_config(args: argparse.Namespace) -> argparse.Namespace:
    args.bootstrap_servers = args.bootstrap_servers or os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    args.topic = args.topic or os.getenv("KAFKA_TOPIC")
    args.output = args.output or os.getenv("KAFKA_OUTPUT_PATH")
    args.checkpoint = args.checkpoint or os.getenv("KAFKA_CHECKPOINT_PATH")
    args.starting_offsets = os.getenv("KAFKA_STARTING_OFFSETS", args.starting_offsets)
    args.run_mode = os.getenv("KAFKA_RUN_MODE", args.run_mode)
    args.kafka_security_protocol = args.kafka_security_protocol or os.getenv("KAFKA_SECURITY_PROTOCOL")
    args.kafka_sasl_mechanism = args.kafka_sasl_mechanism or os.getenv("KAFKA_SASL_MECHANISM")
    args.kafka_sasl_jaas_config = args.kafka_sasl_jaas_config or os.getenv("KAFKA_SASL_JAAS_CONFIG")

    if args.timeout_seconds is None and os.getenv("KAFKA_TIMEOUT_SECONDS"):
        args.timeout_seconds = int(os.environ["KAFKA_TIMEOUT_SECONDS"])

    missing = [
        name
        for name, value in {
            "KAFKA_BOOTSTRAP_SERVERS or --bootstrap-servers": args.bootstrap_servers,
            "KAFKA_TOPIC or --topic": args.topic,
            "KAFKA_OUTPUT_PATH or --output": args.output,
            "KAFKA_CHECKPOINT_PATH or --checkpoint": args.checkpoint,
        }.items()
        if not value
    ]
    if missing:
        raise ValueError(f"Missing required Kafka flatten settings: {', '.join(missing)}")

    return args


def main() -> None:
    args = resolve_config(parse_args())
    spark = (
        SparkSession.builder.appName("kafka-streaming-flatten")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    reader = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", args.topic)
        .option("startingOffsets", args.starting_offsets)
        .option("failOnDataLoss", "false")
    )

    if args.kafka_security_protocol:
        reader = reader.option("kafka.security.protocol", args.kafka_security_protocol)
    if args.kafka_sasl_mechanism:
        reader = reader.option("kafka.sasl.mechanism", args.kafka_sasl_mechanism)
    if args.kafka_sasl_jaas_config:
        reader = reader.option("kafka.sasl.jaas.config", args.kafka_sasl_jaas_config)

    kafka_messages = reader.load()

    parsed = kafka_messages.select(
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp").alias("kafka_timestamp"),
        from_json(col("value").cast("string"), APPLICATION_SCHEMA).alias("payload"),
    ).filter(col("payload.application_id").isNotNull())

    flattened = (
        parsed.withColumn("document", explode_outer("payload.documents"))
        .select(
            col("topic"),
            col("partition"),
            col("offset"),
            col("kafka_timestamp"),
            col("payload.application_id").alias("application_id"),
            col("payload.customer.customer_id").alias("customer_id"),
            col("payload.customer.region").alias("region_code"),
            col("payload.loan.amount").alias("loan_amount"),
            col("payload.loan.term_months").alias("term_months"),
            col("payload.scoring.score").alias("credit_score"),
            col("payload.scoring.risk_level").alias("risk_level"),
            col("payload.decision_status").alias("decision_status"),
            to_timestamp("payload.submitted_at").alias("submitted_at"),
            col("document.type").alias("document_type"),
            col("document.status").alias("document_status"),
        )
    )

    writer = (
        flattened.writeStream.format("parquet")
        .option("path", args.output)
        .option("checkpointLocation", args.checkpoint)
        .outputMode("append")
    )

    if args.run_mode == "once":
        writer = writer.trigger(once=True)

    query = writer.start()

    if args.timeout_seconds:
        query.awaitTermination(args.timeout_seconds)
        if query.isActive:
            query.stop()
    else:
        query.awaitTermination()


if __name__ == "__main__":
    main()
