import argparse
import csv
from datetime import datetime, timedelta
from pathlib import Path
from random import choice, randint, random, seed


REGIONS = ["DE-HE", "DE-BE", "DE-BY", "DE-HH", "DE-NW", "DE-SN"]
PRODUCT_TYPES = ["cash_loan", "credit_card", "mortgage", "car_loan"]
RISK_LEVELS = ["low", "medium", "high"]
DECISIONS = ["approved", "rejected", "manual_review"]
CHANNELS = ["mobile", "web", "office", "partner"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate loan applications CSV for PySpark batch homework.")
    parser.add_argument("--output", required=True, help="Output CSV path.")
    parser.add_argument("--target-mb", type=int, default=60, help="Approximate target file size in MB.")
    parser.add_argument("--seed", type=int, default=84, help="Random seed.")
    return parser.parse_args()


def decision_for_score(score: int) -> str:
    if score >= 700:
        return choice(["approved", "approved", "manual_review"])
    if score >= 560:
        return choice(["manual_review", "approved", "rejected"])
    return choice(["rejected", "manual_review"])


def risk_for_score(score: int) -> str:
    if score >= 700:
        return "low"
    if score >= 560:
        return "medium"
    return "high"


def main() -> None:
    args = parse_args()
    seed(args.seed)

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    target_bytes = args.target_mb * 1024 * 1024
    base_time = datetime(2026, 5, 1, 0, 0, 0)

    with output.open("w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(
            [
                "application_id",
                "event_time",
                "customer_id",
                "region_code",
                "product_type",
                "requested_amount",
                "term_months",
                "credit_score",
                "risk_level",
                "decision_status",
                "approved_amount",
                "channel",
                "employee_review_flag",
                "processing_time_sec",
            ]
        )

        row_number = 1
        while file.tell() < target_bytes:
            score = randint(420, 850)
            risk_level = risk_for_score(score)
            decision = decision_for_score(score)
            requested_amount = randint(1_000, 80_000)
            approved_amount = requested_amount if decision == "approved" else 0
            if decision == "manual_review":
                approved_amount = randint(0, requested_amount)

            writer.writerow(
                [
                    f"app_202605{(row_number % 31) + 1:02d}_{row_number:09d}",
                    (base_time + timedelta(seconds=row_number * randint(20, 120))).strftime("%Y-%m-%d %H:%M:%S"),
                    f"cust_{randint(10000, 999999)}",
                    choice(REGIONS),
                    choice(PRODUCT_TYPES),
                    requested_amount,
                    choice([6, 12, 24, 36, 48, 60]),
                    score,
                    risk_level,
                    decision,
                    approved_amount,
                    choice(CHANNELS),
                    str(decision == "manual_review" or random() < 0.08).lower(),
                    randint(5, 600),
                ]
            )
            row_number += 1

    print(f"Generated {output} with {row_number - 1} rows and {output.stat().st_size / 1024 / 1024:.2f} MB")


if __name__ == "__main__":
    main()
