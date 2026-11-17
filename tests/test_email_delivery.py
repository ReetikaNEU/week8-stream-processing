import json
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from botocore.exceptions import ClientError

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lambdas" / "window_aggregator"))

from email_delivery import process_email_batch


class EmailDeliveryTests(unittest.TestCase):

    def setUp(self):
        self.watermarks = MagicMock()
        self.mailbox = MagicMock()
        self.sqs = MagicMock()

        self.watermarks.get_item.return_value = {
            "Item": {
                "watermark": "2026-11-17T12:02:00+00:00"
            }
        }

    def email(self, event_time, message_id="msg-001"):
        return {
            "record_type": "email",
            "message_id": message_id,
            "recipient": "bob@example.com",
            "event_time": event_time,
            "ingested_at": "2026-11-17T12:05:00Z",
        }

    def process(self, emails):
        return process_email_batch(
            "shard-001",
            emails,
            self.watermarks,
            self.mailbox,
            self.sqs,
            "test-late-queue",
        )

    def test_on_time_email(self):
        email = self.email("2026-11-17T12:02:20Z")

        with patch("email_delivery.save_email", return_value=True) as save:
            result = self.process([email])

        self.assertEqual(result["delivered"], 1)
        self.assertEqual(result["late_queued"], 0)
        save.assert_called_once()
        self.sqs.send_message.assert_not_called()

    def test_duplicate_email(self):
        email = self.email("2026-11-17T12:02:20Z")

        with patch("email_delivery.save_email", return_value=False):
            result = self.process([email])

        self.assertEqual(result["duplicate"], 1)
        self.assertEqual(result["delivered"], 0)
        self.sqs.send_message.assert_not_called()

    def test_late_email(self):
        email = self.email("2026-11-17T12:00:10Z")

        with patch("email_delivery.save_email") as save:
            result = self.process([email])

        self.assertEqual(result["late_queued"], 1)
        save.assert_not_called()

        message = self.sqs.send_message.call_args.kwargs
        self.assertEqual(message["QueueUrl"], "test-late-queue")

        body = json.loads(message["MessageBody"])
        self.assertEqual(body["record_type"], "email")
        self.assertEqual(body["event"]["message_id"], "msg-001")

    def test_late_within_same_batch(self):
        newer = self.email(
            "2026-11-17T12:05:00Z", "newer"
        )
        older = self.email(
            "2026-11-17T12:02:20Z", "older"
        )

        with patch("email_delivery.save_email", return_value=True):
            result = self.process([newer, older])

        self.assertEqual(result["delivered"], 1)
        self.assertEqual(result["late_queued"], 1)

        body = json.loads(
            self.sqs.send_message.call_args.kwargs["MessageBody"]
        )
        self.assertEqual(body["event"]["message_id"], "older")

    def test_watermark_conditional_rejection(self):
        self.watermarks.update_item.side_effect = ClientError(
            {
                "Error": {
                    "Code": "ConditionalCheckFailedException",
                    "Message": "Newer watermark already exists",
                }
            },
            "UpdateItem",
        )

        email = self.email("2026-11-17T12:02:20Z")

        with patch("email_delivery.save_email", return_value=True):
            result = self.process([email])

        self.assertEqual(result["delivered"], 1)
        condition = self.watermarks.update_item.call_args.kwargs[
            "ConditionExpression"
        ]
        self.assertIn("attribute_not_exists", condition)


if __name__ == "__main__":
    unittest.main()
