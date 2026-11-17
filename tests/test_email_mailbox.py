import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

from botocore.exceptions import ClientError

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lambdas" / "window_aggregator"))

from email_mailbox import save_email


class EmailMailboxTests(unittest.TestCase):

    def setUp(self):
        self.table = MagicMock()
        self.now = datetime(
            2026, 11, 17, 14, 30, 5,
            tzinfo=timezone.utc,
        )
        self.email = {
            "message_id": "msg-001",
            "sender": "alice@example.com",
            "recipient": "BOB@example.com",
            "subject": "Meeting tomorrow",
            "body": "Hello Bob",
            "date": "2026-11-17T09:30:00-05:00",
            "event_time": "2026-11-17T09:30:00-05:00",
            "ingested_at": "2026-11-17T14:30:03Z",
        }

    def test_first_delivery(self):
        result = save_email(
            self.email, self.table,
            processed_at=self.now,
        )

        self.assertTrue(result)
        self.table.put_item.assert_called_once()

        call = self.table.put_item.call_args.kwargs
        item = call["Item"]

        self.assertEqual(item["recipient"], "bob@example.com")
        self.assertEqual(item["message_id"], "msg-001")
        self.assertEqual(item["folder"], "INBOX")
        self.assertEqual(item["delivery_latency_ms"], 2000)
        self.assertEqual(
            item["event_time_id"],
            "2026-11-17T14:30:00.000000Z#msg-001",
        )
        self.assertIn(
            "attribute_not_exists(message_id)",
            call["ConditionExpression"],
        )

    def test_duplicate_collapsed(self):
        self.table.put_item.side_effect = ClientError(
            {
                "Error": {
                    "Code": "ConditionalCheckFailedException",
                    "Message": "Already exists",
                }
            },
            "PutItem",
        )

        result = save_email(
            self.email, self.table,
            processed_at=self.now,
        )

        self.assertFalse(result)
        self.table.put_item.assert_called_once()

    def test_same_id_different_recipient(self):
        save_email(self.email, self.table, processed_at=self.now)

        second = dict(self.email)
        second["recipient"] = "charlie@example.com"

        save_email(second, self.table, processed_at=self.now)

        calls = self.table.put_item.call_args_list
        self.assertEqual(len(calls), 2)
        self.assertEqual(
            calls[0].kwargs["Item"]["recipient"],
            "bob@example.com",
        )
        self.assertEqual(
            calls[1].kwargs["Item"]["recipient"],
            "charlie@example.com",
        )

    def test_late_folder(self):
        result = save_email(
            self.email, self.table,
            folder="LATE",
            processed_at=self.now,
        )

        self.assertTrue(result)
        item = self.table.put_item.call_args.kwargs["Item"]
        self.assertEqual(item["folder"], "LATE")

    def test_unexpected_aws_error(self):
        self.table.put_item.side_effect = ClientError(
            {
                "Error": {
                    "Code": "AccessDeniedException",
                    "Message": "Access denied",
                }
            },
            "PutItem",
        )

        with self.assertRaises(ClientError):
            save_email(
                self.email, self.table,
                processed_at=self.now,
            )


if __name__ == "__main__":
    unittest.main()
