"""Replay email datasets through the Week 8 email API."""

import argparse
import csv
import json
import os
import ssl
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


def load_messages(path):
    if path.suffix.lower() == ".jsonl":
        with path.open(encoding="utf-8") as file:
            records = [
                json.loads(line)
                for line in file
                if line.strip()
            ]
    elif path.suffix.lower() == ".csv":
        with path.open(encoding="utf-8-sig", newline="") as file:
            records = list(csv.DictReader(file))
    else:
        raise ValueError("Use a .jsonl or .csv file")

    aliases = {
        "message_id": ("message_id", "messageid", "id"),
        "sender": ("sender", "from"),
        "recipient": ("recipient", "to"),
        "subject": ("subject",),
        "body": ("body", "text", "content"),
        "date": ("date",),
    }

    normalized = []

    for number, record in enumerate(records, 1):
        fields = {
            str(key).strip().lower()
            .replace("-", "_").replace(" ", "_"): value
            for key, value in record.items()
        }

        email = {}

        for target, alternatives in aliases.items():
            email[target] = next(
                (
                    fields[name]
                    for name in alternatives
                    if fields.get(name) is not None
                ),
                "",
            )

        missing = [
            field
            for field in (
                "message_id", "sender", "recipient",
                "subject", "date",
            )
            if not email[field]
        ]

        if missing:
            raise ValueError(
                f"Row {number} missing: {', '.join(missing)}"
            )

        normalized.append(email)

    return normalized


def send_email(endpoint, email, context):
    request = Request(
        endpoint,
        data=json.dumps(email).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urlopen(
            request,
            timeout=15,
            context=context,
        ) as result:
            return result.status, email["message_id"], ""

    except HTTPError as error:
        return (
            error.code,
            email["message_id"],
            error.read().decode("utf-8", errors="replace"),
        )

    except (URLError, TimeoutError) as error:
        return 0, email["message_id"], str(error)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("dataset", type=Path)
    parser.add_argument("--count", type=int)
    parser.add_argument("--rate", type=float, default=10)
    parser.add_argument("--workers", type=int, default=20)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if args.rate <= 0 or args.workers <= 0:
        parser.error("Rate and workers must be positive")
    if args.count is not None and args.count < 0:
        parser.error("Count must not be negative")

    emails = load_messages(args.dataset)

    if args.count is not None:
        emails = emails[:args.count]

    print("Loaded emails:", len(emails))
    print("Unique Message-IDs:", len({
        email["message_id"] for email in emails
    }))

    if args.dry_run:
        print("DRY RUN: no AWS requests sent")
        if emails:
            print("First email:", json.dumps(emails[0]))
        return

    endpoint = os.environ.get("API_ENDPOINT", "").strip()

    if not endpoint.startswith("https://") or "](" in endpoint:
        raise SystemExit("Set a valid API_ENDPOINT first")

    endpoint = endpoint.rstrip("/") + "/emails"

    try:
        import certifi
        context = ssl.create_default_context(
            cafile=certifi.where()
        )
    except ImportError:
        context = ssl.create_default_context()

    futures = []
    started = time.monotonic()

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        for index, email in enumerate(emails):
            target_time = started + index / args.rate
            delay = target_time - time.monotonic()

            if delay > 0:
                time.sleep(delay)

            futures.append(
                pool.submit(send_email, endpoint, email, context)
            )

        counts = Counter()
        failures = []

        for future in as_completed(futures):
            status, message_id, details = future.result()
            counts[status] += 1

            if status != 202:
                failures.append((message_id, status, details))

    print("Submitted:", len(emails))
    print("HTTP 202 accepted:", counts[202])
    print("Other responses:", {
        code: count
        for code, count in counts.items()
        if code != 202
    })
    print("Elapsed seconds:", round(time.monotonic() - started, 2))

    for message_id, status, details in failures[:5]:
        print("FAILED:", message_id, status, details)

    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
