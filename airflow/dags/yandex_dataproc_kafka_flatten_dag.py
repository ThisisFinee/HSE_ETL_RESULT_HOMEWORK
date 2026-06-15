from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.yandex.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocCreatePysparkJobOperator,
    DataprocDeleteClusterOperator,
)
from airflow.utils.trigger_rule import TriggerRule


FOLDER_ID = Variable.get("YC_FOLDER_ID")
SUBNET_ID = Variable.get("YC_SUBNET_ID")
SERVICE_ACCOUNT_ID = Variable.get("YC_SERVICE_ACCOUNT_ID")
ZONE = Variable.get("YC_ZONE", default_var="ru-central1-a")
SSH_PUBLIC_KEY = Variable.get("YC_SSH_PUBLIC_KEY")

BUCKET = Variable.get("S3_BUCKET")
SCRIPT_PATH = Variable.get("KAFKA_SCRIPT_PATH", default_var=f"s3a://{BUCKET}/scripts/kafka_streaming_flatten.py")
OUTPUT_PATH = Variable.get("KAFKA_OUTPUT_PATH", default_var=f"s3a://{BUCKET}/processed/kafka_flat/")
CHECKPOINT_PATH = Variable.get("KAFKA_CHECKPOINT_PATH", default_var=f"s3a://{BUCKET}/checkpoints/kafka_flat/")
CHECKPOINT_RUN_PATH = f"{CHECKPOINT_PATH.rstrip('/')}/{{{{ ts_nodash }}}}"

KAFKA_BOOTSTRAP_SERVERS = Variable.get("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = Variable.get("KAFKA_TOPIC", default_var="loan_applications")
KAFKA_SECURITY_PROTOCOL = Variable.get("KAFKA_SECURITY_PROTOCOL", default_var="")
KAFKA_SASL_MECHANISM = Variable.get("KAFKA_SASL_MECHANISM", default_var="")
KAFKA_SASL_JAAS_CONFIG = Variable.get("KAFKA_SASL_JAAS_CONFIG", default_var="")
KAFKA_USERNAME = Variable.get("KAFKA_USERNAME", default_var="")
KAFKA_PASSWORD = Variable.get("KAFKA_PASSWORD", default_var="")

CLUSTER_NAME = Variable.get("KAFKA_DATAPROC_CLUSTER_NAME", default_var="etl-homework-kafka-dataproc")
WORKER_COUNT = int(Variable.get("KAFKA_DATAPROC_WORKER_COUNT", default_var="1"))
RESOURCE_PRESET = Variable.get("DATAPROC_RESOURCE_PRESET", default_var="s2.small")

SPARK_KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2"


def build_jaas_config() -> str:
    if KAFKA_SASL_JAAS_CONFIG:
        return KAFKA_SASL_JAAS_CONFIG
    if KAFKA_USERNAME and KAFKA_PASSWORD:
        return (
            "org.apache.kafka.common.security.scram.ScramLoginModule required "
            f'username="{KAFKA_USERNAME}" password="{KAFKA_PASSWORD}";'
        )
    return ""


def build_job_args() -> list[str]:
    args = [
        "--bootstrap-servers",
        KAFKA_BOOTSTRAP_SERVERS,
        "--topic",
        KAFKA_TOPIC,
        "--output",
        OUTPUT_PATH,
        "--checkpoint",
        CHECKPOINT_RUN_PATH,
        "--starting-offsets",
        "earliest",
        "--run-mode",
        "once",
        "--timeout-seconds",
        "900",
    ]

    if KAFKA_SECURITY_PROTOCOL:
        args.extend(["--kafka-security-protocol", KAFKA_SECURITY_PROTOCOL])
    if KAFKA_SASL_MECHANISM:
        args.extend(["--kafka-sasl-mechanism", KAFKA_SASL_MECHANISM])

    jaas_config = build_jaas_config()
    if jaas_config:
        args.extend(["--kafka-sasl-jaas-config", jaas_config])

    return args


with DAG(
    dag_id="yandex_dataproc_kafka_flatten",
    description="Create Data Processing cluster, flatten accumulated Kafka messages and delete the cluster.",
    start_date=datetime(2026, 5, 1),
    schedule=None,
    catchup=False,
    default_args={
        "owner": "etl-homework",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["etl-homework", "yandex-dataproc", "kafka", "pyspark"],
) as dag:
    start = EmptyOperator(task_id="start")

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_dataproc_cluster",
        folder_id=FOLDER_ID,
        cluster_name=CLUSTER_NAME,
        cluster_description="Temporary cluster for Kafka flatten homework job",
        ssh_public_keys=[SSH_PUBLIC_KEY],
        subnet_id=SUBNET_ID,
        zone=ZONE,
        service_account_id=SERVICE_ACCOUNT_ID,
        services=["HDFS", "YARN", "SPARK"],
        cluster_image_version="2.1",
        masternode_resource_preset=RESOURCE_PRESET,
        masternode_disk_type="network-ssd",
        masternode_disk_size=32,
        computenode_resource_preset=RESOURCE_PRESET,
        computenode_disk_type="network-ssd",
        computenode_disk_size=32,
        computenode_count=WORKER_COUNT,
    )

    run_kafka_flatten = DataprocCreatePysparkJobOperator(
        task_id="run_kafka_streaming_flatten_once",
        cluster_id="{{ ti.xcom_pull(task_ids='create_dataproc_cluster') }}",
        main_python_file_uri=SCRIPT_PATH,
        args=build_job_args(),
        properties={
            "spark.submit.packages": SPARK_KAFKA_PACKAGE,
            "spark.jars.packages": SPARK_KAFKA_PACKAGE,
            "spark.jars.repositories": "https://repo1.maven.org/maven2",
            "spark.sql.streaming.forceDeleteTempCheckpointLocation": "true",
        },
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_dataproc_cluster",
        cluster_id="{{ ti.xcom_pull(task_ids='create_dataproc_cluster') }}",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    finish = EmptyOperator(task_id="finish", trigger_rule=TriggerRule.ALL_DONE)

    start >> create_cluster >> run_kafka_flatten >> delete_cluster >> finish
