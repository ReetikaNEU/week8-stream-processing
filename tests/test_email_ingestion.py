import json
import os
import sys
import unittest
from pathlib import Path
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lambdas" / "event_producer"))

os.environ.setdefault("CLICKSTREAM_NAME", "test-stream")
os.environ.setdefault("EMAIL_QUARANTINE_TABLE", "test-quarantine")

with patch("boto3.client"):
    import handler as producer


class EmailIngestionTests(unittest.TestCase):

    def setUp(self):
        self.email = {
            "message_id": "msg-001",
            "sender": "alice@example.com",
            "recipient": "BOB@example.com",
            "subject": "Meeting tomorrow",
            "body": "Hello Bob",
            "date": (
                datetime.now(timezone.utc)
                - timedelta(minutes=1)
            ).isoformat()
        }

    def test_valid_email(self):
        event = {
            "path": "/emails",
            "body": json.dumps(self.email)
        }

        with patch.object(producer, "kinesis_client") as mock, patch("handler.boto3.resource") as resource:
            response = producer.lambda_handler(event, None)

            self.assertEqual(response["statusCode"], 202)
            mock.put_record.assert_called_once()
            resource.return_value.Table.return_value.put_item.assert_not_called()

            args = mock.put_record.call_args.kwargs
            self.assertEqual(
                args["PartitionKey"],
                "bob@example.com"
            )

            record = json.loads(args["Data"])
            self.assertEqual(record["record_type"], "email")
            self.assertEqual(record["message_id"], "msg-001")
            self.assertIn("ingested_at", record)
            self.assertIn("event_time", record)

    def test_invalid_email(self):
        del self.email["date"]

        event = {
            "path": "/emails",
            "body": json.dumps(self.email)
        }

        with patch.object(producer, "kinesis_client") as mock, patch("handler.boto3.resource") as resource:
            response = producer.lambda_handler(event, None)

            self.assertEqual(response["statusCode"], 400)
            mock.put_record.assert_not_called()

            table = resource.return_value.Table.return_value
            table.put_item.assert_called_once()

            item = table.put_item.call_args.kwargs["Item"]
            self.assertEqual(item["status"], "QUARANTINED")
            self.assertTrue(item["reasons"])

    def test_invalid_json(self):
        event = {
            "path": "/emails",
            "body": "{invalid-json"
        }

        with patch.object(producer, "kinesis_client") as mock, patch("handler.boto3.resource") as resource:
            response = producer.lambda_handler(event, None)

            self.assertEqual(response["statusCode"], 400)
            mock.put_record.assert_not_called()

            table = resource.return_value.Table.return_value
            table.put_item.assert_called_once()

            item = table.put_item.call_args.kwargs["Item"]
            self.assertEqual(item["status"], "QUARANTINED")
            self.assertTrue(item["reasons"])


if __name__ == "__main__":
    unittest.main()
