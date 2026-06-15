from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from random import choice, randint, seed

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from kafka import KafkaProducer


REGIONS = ["DE-HE", "DE-BE", "DE-BY", "DE-HH", "DE-NW", "DE-SN"]
DECISIONS = ["approved", "rejected", "manual_review"]
DOCUMENT_TYPES = ["passport", "income_statement", "employment_contract", "bank_statement"]
DOCUMENT_STATUSES = ["verified", "pending", "rejected"]

KAFKA_BOOTSTRAP_SERVERS = Variable.get("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = Variable.get("KAFKA_TOPIC", default_var="loan_applications")
TARGET_MB = int(Variable.get("KAFKA_PRODUCER_TARGET_MB", default_var="25"))
RANDOM_SEED = int(Variable.get("KAFKA_PRODUCER_RANDOM_SEED", default_var="126"))

KAFKA_SECURITY_PROTOCOL = Variable.get("KAFKA_SECURITY_PROTOCOL", default_var="PLAINTEXT")
KAFKA_SASL_MECHANISM = Variable.get("KAFKA_SASL_MECHANISM", default_var="")
KAFKA_USERNAME = Variable.get("KAFKA_USERNAME", default_var="")
KAFKA_PASSWORD = Variable.get("KAFKA_PASSWORD", default_var="")


def build_event(row_number: int) -> dict:
    score = randint(420, 850)
    risk_level = "low" if score >= 700 else "medium" if score >= 560 else "high"
    submitted_at = datetime(2026, 5, 1, tzinfo=timezone.utc) + timedelta(seconds=row_number * randint(10, 75))

    return {
        "application_id": f"loan_{row_number:09d}",
        "customer": {
            "customer_id": f"cust_{randint(10000, 999999)}",
            "region": choice(REGIONS),
        },
        "loan": {
            "amount": randint(1_000, 80_000),
            "term_months": choice([6, 12, 24, 36, 48, 60]),
        },
        "scoring": {
            "score": score,
            "risk_level": risk_level,
        },
        "documents": [
            {
                "type": choice(DOCUMENT_TYPES),
                "status": choice(DOCUMENT_STATUSES),
            }
            for _ in range(randint(1, 3))
        ],
        "decision_status": choice(DECISIONS),
        "submitted_at": submitted_at.isoformat().replace("+00:00", "Z"),
    }


def build_producer() -> KafkaProducer:
    options = {
        "bootstrap_servers": [server.strip() for server in KAFKA_BOOTSTRAP_SERVERS.split(",")],
        "value_serializer": lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
        "key_serializer": lambda value: value.encode("utf-8"),
        "security_protocol": KAFKA_SECURITY_PROTOCOL,
    }

    if KAFKA_SASL_MECHANISM:
        options.update(
            {
                "sasl_mechanism": KAFKA_SASL_MECHANISM,
                "sasl_plain_username": KAFKA_USERNAME,
                "sasl_plain_password": KAFKA_PASSWORD,
            }
        )

    return KafkaProducer(**options)


def produce_kafka_messages() -> dict:
    seed(RANDOM_SEED)
    producer = build_producer()
    target_bytes = TARGET_MB * 1024 * 1024
    sent_bytes = 0
    row_number = 1

    while sent_bytes < target_bytes:
        event = build_event(row_number)
        payload_size = len(json.dumps(event, ensure_ascii=False).encode("utf-8"))
        producer.send(KAFKA_TOPIC, key=event["application_id"], value=event)
        sent_bytes += payload_size
        row_number += 1

        if row_number % 5000 == 0:
            producer.flush()
            print(f"Sent {row_number - 1} messages, {sent_bytes / 1024 / 1024:.2f} MB")

    producer.flush()
    producer.close()

    result = {
        "topic": KAFKA_TOPIC,
        "messages": row_number - 1,
        "sent_mb": round(sent_bytes / 1024 / 1024, 2),
    }
    print(json.dumps(result))
    return result


with DAG(
    dag_id="yandex_kafka_producer",
    description="Generate nested loan application events and write them directly to Kafka from Airflow.",
    start_date=datetime(2026, 5, 1),
    schedule=None,
    catchup=False,
    default_args={
        "owner": "etl-homework",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["etl-homework", "kafka", "producer"],
) as dag:
    start = EmptyOperator(task_id="start")

    produce_messages = PythonOperator(
        task_id="produce_kafka_messages",
        python_callable=produce_kafka_messages,
        execution_timeout=timedelta(minutes=20),
    )

    finish = EmptyOperator(task_id="finish")

    start >> produce_messages >> finish
