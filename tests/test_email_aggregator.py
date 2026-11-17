import base64
import importlib.util
import json
import os
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
AGG_DIR = ROOT / "lambdas" / "window_aggregator"
sys.path.insert(0, str(AGG_DIR))

ENV = {
    "WINDOW_TABLE": "test-windows",
    "WATERMARK_TABLE": "test-watermarks",
    "LATE_QUEUE_URL": "test-late-queue",
    "EMAIL_MAILBOX_TABLE": "test-mailboxes",
    "WINDOW_SIZE_SECONDS": "60",
    "GRACE_PERIOD_SECONDS": "30",
}

with patch.dict(os.environ, ENV):
    with patch("boto3.resource"), patch("boto3.client"):
        spec = importlib.util.spec_from_file_location(
            "week8_aggregator_under_test",
            AGG_DIR / "handler.py",
        )
        aggregator = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(aggregator)


class EmailAggregatorTests(unittest.TestCase):

    def setUp(self):
        now = datetime.now(timezone.utc).isoformat()

        self.click = {
            "event_id": "click-001",
            "user_id": "user-001",
            "event_type": "page_view",
            "event_time": now,
        }

        self.email = {
            "record_type": "email",
            "message_id": "email-001",
            "sender": "alice@example.com",
            "recipient": "bob@example.com",
            "subject": "Meeting",
            "date": now,
            "event_time": now,
            "ingested_at": now,
        }

    def record(self, payload, sequence):
        data = base64.b64encode(
            json.dumps(payload).encode("utf-8")
        ).decode("utf-8")

        return {
            "eventID": f"shardId-000000000000:{sequence}",
            "kinesis": {
                "data": data,
                "sequenceNumber": str(sequence),
                "approximateArrivalTimestamp": (
                    datetime.now(timezone.utc).timestamp()
                ),
            },
        }

    def run_handler(self, records):
        return aggregator.lambda_handler(
            {"Records": records},
            None,
        )

    def test_original_clickstream_path(self):
        with patch.object(aggregator, "_process_shard_batch") as clicks:
            with patch.object(aggregator, "process_email_batch") as emails:
                result = self.run_handler([
                    self.record(self.click, 1)
                ])

        self.assertEqual(result, {"batchItemFailures": []})
        clicks.assert_called_once()
        emails.assert_not_called()

    def test_email_path(self):
        counts = {
            "delivered": 1,
            "duplicate": 0,
            "late_queued": 0,
        }

        with patch.object(aggregator, "_process_shard_batch") as clicks:
            with patch.object(
                aggregator,
                "process_email_batch",
                return_value=counts,
            ) as emails:
                result = self.run_handler([
                    self.record(self.email, 2)
                ])

        self.assertEqual(result, {"batchItemFailures": []})
        emails.assert_called_once()
        clicks.assert_not_called()

    def test_mixed_batch(self):
        counts = {
            "delivered": 1,
            "duplicate": 0,
            "late_queued": 0,
        }

        records = [
            self.record(self.click, 3),
            self.record(self.email, 4),
        ]

        with patch.object(aggregator, "_process_shard_batch") as clicks:
            with patch.object(
                aggregator,
                "process_email_batch",
                return_value=counts,
            ) as emails:
                self.run_handler(records)

        clicks.assert_called_once()
        emails.assert_called_once()

        click_records = clicks.call_args.args[1]
        email_records = emails.call_args.args[1]

        self.assertEqual(len(click_records), 1)
        self.assertEqual(len(email_records), 1)
        self.assertEqual(
            email_records[0]["message_id"],
            "email-001",
        )

    def test_malformed_email_not_delivered(self):
        malformed = dict(self.email)
        del malformed["date"]

        with patch.object(aggregator, "_process_shard_batch") as clicks:
            with patch.object(aggregator, "process_email_batch") as emails:
                self.run_handler([
                    self.record(malformed, 5)
                ])

        clicks.assert_not_called()
        emails.assert_not_called()

    def test_missing_mailbox_configuration_fails(self):
        with patch.object(aggregator, "mailbox_table", None):
            with self.assertRaises(RuntimeError):
                self.run_handler([
                    self.record(self.email, 6)
                ])


if __name__ == "__main__":
    unittest.main()
