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
INPUT_PATH = Variable.get("S3_INPUT_PATH", default_var=f"s3a://{BUCKET}/raw/applications/")
OUTPUT_PATH = Variable.get("S3_OUTPUT_PATH", default_var=f"s3a://{BUCKET}/processed/applications/")
SCRIPT_PATH = Variable.get("S3_SCRIPT_PATH", default_var=f"s3a://{BUCKET}/scripts/applications_batch_etl.py")

CLUSTER_NAME = Variable.get("DATAPROC_CLUSTER_NAME", default_var="etl-homework-dataproc")
WORKER_COUNT = int(Variable.get("DATAPROC_WORKER_COUNT", default_var="2"))
RESOURCE_PRESET = Variable.get("DATAPROC_RESOURCE_PRESET", default_var="s2.small")


with DAG(
    dag_id="yandex_dataproc_applications_etl",
    description="Create Yandex Data Processing cluster, run PySpark ETL and delete the cluster.",
    start_date=datetime(2026, 5, 1),
    schedule=None,
    catchup=False,
    default_args={
        "owner": "etl-homework",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["etl-homework", "yandex-dataproc", "pyspark"],
) as dag:
    start = EmptyOperator(task_id="start")

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_dataproc_cluster",
        folder_id=FOLDER_ID,
        cluster_name=CLUSTER_NAME,
        cluster_description="Temporary cluster for ETL homework batch processing",
        ssh_public_keys=[SSH_PUBLIC_KEY],
        subnet_id=SUBNET_ID,
        zone=ZONE,
        service_account_id=SERVICE_ACCOUNT_ID,
        services=["HDFS", "YARN", "SPARK"],
        cluster_image_version="2.1",
        masternode_resource_preset=RESOURCE_PRESET,
        masternode_disk_type="network-ssd",
        masternode_disk_size=64,
        computenode_resource_preset=RESOURCE_PRESET,
        computenode_disk_type="network-ssd",
        computenode_disk_size=64,
        computenode_count=WORKER_COUNT,
    )

    run_pyspark = DataprocCreatePysparkJobOperator(
        task_id="run_applications_batch_etl",
        cluster_id="{{ ti.xcom_pull(task_ids='create_dataproc_cluster') }}",
        main_python_file_uri=SCRIPT_PATH,
        args=[
            "--input",
            INPUT_PATH,
            "--output",
            OUTPUT_PATH,
        ],
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_dataproc_cluster",
        cluster_id="{{ ti.xcom_pull(task_ids='create_dataproc_cluster') }}",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    finish = EmptyOperator(task_id="finish", trigger_rule=TriggerRule.ALL_DONE)

    start >> create_cluster >> run_pyspark >> delete_cluster >> finish
