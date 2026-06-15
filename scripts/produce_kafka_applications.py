import argparse
import json
import os
from datetime import datetime, timedelta, timezone
from random import choice, randint, seed

from kafka import KafkaProducer


REGIONS = ["DE-HE", "DE-BE", "DE-BY", "DE-HH", "DE-NW", "DE-SN"]
RISK_LEVELS = ["low", "medium", "high"]
DECISIONS = ["approved", "rejected", "manual_review"]
DOCUMENT_TYPES = ["passport", "income_statement", "employment_contract", "bank_statement"]
DOCUMENT_STATUSES = ["verified", "pending", "rejected"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Produce nested loan application JSON messages to Kafka.")
    parser.add_argument("--bootstrap-servers", required=True, help="Kafka bootstrap servers, comma separated.")
    parser.add_argument("--topic", required=True, help="Kafka topic name.")
    parser.add_argument("--target-mb", type=int, default=25, help="Approximate total payload size in MB.")
    parser.add_argument("--security-protocol", default="PLAINTEXT", help="Kafka security protocol.")
    parser.add_argument("--sasl-mechanism", default=None, help="Optional SASL mechanism.")
    parser.add_argument("--sasl-username", default=None, help="Optional SASL username.")
    parser.add_argument("--sasl-password", default=None, help="Optional SASL password.")
    parser.add_argument("--seed", type=int, default=126, help="Random seed.")
    return parser.parse_args()


def build_event(row_number: int) -> dict:
    score = randint(420, 850)
    risk_level = "low" if score >= 700 else "medium" if score >= 560 else "high"
    document_count = randint(1, 3)
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
            "risk_level": risk_level if risk_level in RISK_LEVELS else choice(RISK_LEVELS),
        },
        "documents": [
            {
                "type": choice(DOCUMENT_TYPES),
                "status": choice(DOCUMENT_STATUSES),
            }
            for _ in range(document_count)
        ],
        "decision_status": choice(DECISIONS),
        "submitted_at": submitted_at.isoformat().replace("+00:00", "Z"),
    }


def build_producer_options(
    bootstrap_servers: str,
    security_protocol: str = "PLAINTEXT",
    sasl_mechanism: str | None = None,
    sasl_username: str | None = None,
    sasl_password: str | None = None,
) -> dict:
    producer_options = {
        "bootstrap_servers": [server.strip() for server in bootstrap_servers.split(",")],
        "value_serializer": lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
        "key_serializer": lambda value: value.encode("utf-8"),
        "security_protocol": security_protocol,
    }
    if sasl_mechanism:
        producer_options.update(
            {
                "sasl_mechanism": sasl_mechanism,
                "sasl_plain_username": sasl_username,
                "sasl_plain_password": sasl_password,
            }
        )
    return producer_options


def produce_messages(
    bootstrap_servers: str,
    topic: str,
    target_mb: int,
    security_protocol: str = "PLAINTEXT",
    sasl_mechanism: str | None = None,
    sasl_username: str | None = None,
    sasl_password: str | None = None,
    random_seed: int = 126,
) -> dict:
    seed(random_seed)

    producer = KafkaProducer(
        **build_producer_options(
            bootstrap_servers=bootstrap_servers,
            security_protocol=security_protocol,
            sasl_mechanism=sasl_mechanism,
            sasl_username=sasl_username,
            sasl_password=sasl_password,
        )
    )
    target_bytes = target_mb * 1024 * 1024
    sent_bytes = 0
    row_number = 1

    while sent_bytes < target_bytes:
        event = build_event(row_number)
        payload_size = len(json.dumps(event, ensure_ascii=False).encode("utf-8"))
        producer.send(args.topic, key=event["application_id"], value=event)
        sent_bytes += payload_size
        row_number += 1

        if row_number % 5000 == 0:
            producer.flush()
            print(f"Sent {row_number - 1} messages, {sent_bytes / 1024 / 1024:.2f} MB")

    producer.flush()
    producer.close()
    result = {
        "topic": topic,
        "messages": row_number - 1,
        "sent_mb": round(sent_bytes / 1024 / 1024, 2),
    }
    print(f"Finished: sent {result['messages']} messages, {result['sent_mb']:.2f} MB")
    return result


def handler(event, context):
    """Yandex Cloud Functions entrypoint."""
    body = {}
    if isinstance(event, dict) and event.get("body"):
        body = json.loads(event["body"])

    result = produce_messages(
        bootstrap_servers=body.get("bootstrap_servers") or os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        topic=body.get("topic") or os.environ["KAFKA_TOPIC"],
        target_mb=int(body.get("target_mb") or os.getenv("TARGET_MB", "25")),
        security_protocol=body.get("security_protocol") or os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"),
        sasl_mechanism=body.get("sasl_mechanism") or os.getenv("KAFKA_SASL_MECHANISM"),
        sasl_username=body.get("sasl_username") or os.getenv("KAFKA_USERNAME"),
        sasl_password=body.get("sasl_password") or os.getenv("KAFKA_PASSWORD"),
        random_seed=int(body.get("seed") or os.getenv("RANDOM_SEED", "126")),
    )
    return {"statusCode": 200, "body": json.dumps(result)}


def main() -> None:
    args = parse_args()
    produce_messages(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        target_mb=args.target_mb,
        security_protocol=args.security_protocol,
        sasl_mechanism=args.sasl_mechanism,
        sasl_username=args.sasl_username,
        sasl_password=args.sasl_password,
        random_seed=args.seed,
    )


if __name__ == "__main__":
    main()
