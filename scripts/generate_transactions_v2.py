import argparse
import csv
from datetime import datetime, timedelta
from pathlib import Path
from random import choice, randint, random, seed


REGIONS = ["DE-HE", "DE-BE", "DE-BY", "DE-HH", "DE-NW", "DE-SN"]
CAMPAIGNS = ["credit_card_offer", "cash_loan_offer", "mortgage_offer", "insurance_offer"]
CALL_STATUSES = ["answered", "missed", "busy", "failed"]
RESPONSES = ["interested", "not_interested", "callback_requested", "unknown"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate transactions_v2 CSV for YDB/Data Transfer homework.")
    parser.add_argument("--output", required=True, help="Output CSV path.")
    parser.add_argument("--target-mb", type=int, default=35, help="Approximate target file size in MB.")
    parser.add_argument("--seed", type=int, default=42, help="Random seed.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    seed(args.seed)

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    target_bytes = args.target_mb * 1024 * 1024
    base_time = datetime(2026, 5, 1, 8, 0, 0)

    with output.open("w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(
            [
                "call_id",
                "call_time",
                "client_id",
                "region_code",
                "campaign_type",
                "call_status",
                "client_response",
                "duration_sec",
                "follow_up_required",
            ]
        )

        row_number = 1
        while file.tell() < target_bytes:
            call_status = choice(CALL_STATUSES)
            client_response = choice(RESPONSES) if call_status == "answered" else "unknown"
            writer.writerow(
                [
                    f"call_202605{(row_number % 31) + 1:02d}_{row_number:09d}",
                    (base_time + timedelta(seconds=row_number * randint(15, 90))).strftime("%Y-%m-%d %H:%M:%S"),
                    f"client_{randint(1000, 999999)}",
                    choice(REGIONS),
                    choice(CAMPAIGNS),
                    call_status,
                    client_response,
                    randint(10, 900),
                    str(random() < 0.22).lower(),
                ]
            )
            row_number += 1

    print(f"Generated {output} with {row_number - 1} rows and {output.stat().st_size / 1024 / 1024:.2f} MB")


if __name__ == "__main__":
    main()
