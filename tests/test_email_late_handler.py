import importlib.util
import json
import os
import unittest
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch

from botocore.exceptions import ClientError

ROOT = Path(__file__).resolve().parents[1]
HANDLER = ROOT / "lambdas" / "late_data_handler" / "handler.py"

with patch.dict(os.environ, {
    "LATE_TABLE": "test-late-aggregates",
    "EMAIL_MAILBOX_TABLE": "test-email-mailboxes",
}):
    with patch("boto3.resource"):
        spec = importlib.util.spec_from_file_location(
            "week8_late_handler_under_test",
            HANDLER,
        )
        late_handler = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(late_handler)


class EmailLateHandlerTests(unittest.TestCase):

    def setUp(self):
        late_handler.mailbox_table = MagicMock()
        late_handler.late_table = MagicMock()

        self.email_message = {
            "record_type": "email",
            "event": {
                "record_type": "email",
                "message_id": "late-001",
                "sender": "alice@example.com",
                "recipient": "BOB@example.com",
                "subject": "Late invoice",
                "body": "Invoice attached",
                "date": "2026-11-17T09:30:00-05:00",
                "event_time": "2026-11-17T09:30:00-05:00",
                "ingested_at": "2026-11-17T14:35:00Z",
            },
            "window_start": "2026-11-17T14:30:00+00:00",
            "lateness_seconds": 180,
        }

    def sqs_record(self, payload, message_id="sqs-001"):
        return {
            "messageId": message_id,
            "body": json.dumps(payload),
        }

    def invoke(self, records):
        return late_handler.lambda_handler(
            {"Records": records},
            None,
        )

    def test_late_email_stored(self):
        result = self.invoke([
            self.sqs_record(self.email_message)
        ])

        self.assertEqual(result["batchItemFailures"], [])

        table = late_handler.mailbox_table
        table.put_item.assert_called_once()

        args = table.put_item.call_args.kwargs
        item = args["Item"]

        self.assertEqual(item["recipient"], "bob@example.com")
        self.assertEqual(item["message_id"], "late-001")
        self.assertEqual(item["folder"], "LATE")
        self.assertEqual(
            item["event_time_id"],
            "2026-11-17T14:30:00.000000Z#late-001",
        )
        self.assertEqual(item["lateness_seconds"], Decimal("180"))
        self.assertIn(
            "attribute_not_exists(message_id)",
            args["ConditionExpression"],
        )

    def test_duplicate_late_email_collapsed(self):
        late_handler.mailbox_table.put_item.side_effect = ClientError(
            {
                "Error": {
                    "Code": "ConditionalCheckFailedException",
                    "Message": "Already exists",
                }
            },
            "PutItem",
        )

        result = self.invoke([
            self.sqs_record(self.email_message)
        ])

        self.assertEqual(result["batchItemFailures"], [])
        late_handler.mailbox_table.put_item.assert_called_once()

    def test_database_error_requests_sqs_retry(self):
        late_handler.mailbox_table.put_item.side_effect = ClientError(
            {
                "Error": {
                    "Code": "AccessDeniedException",
                    "Message": "Access denied",
                }
            },
            "PutItem",
        )

        with patch.object(late_handler, "logger"):
            result = self.invoke([
                self.sqs_record(
                    self.email_message,
                    message_id="failed-001",
                )
            ])

        self.assertEqual(
            result["batchItemFailures"],
            [{"itemIdentifier": "failed-001"}],
        )

    def test_original_clickstream_late_path(self):
        click_message = {
            "event": {
                "event_id": "click-001",
                "user_id": "user-001",
                "event_type": "click",
                "event_time": "2026-11-17T14:30:00Z",
            },
            "window_start": "2026-11-17T14:30:00+00:00",
            "lateness_seconds": 42,
        }

        result = self.invoke([
            self.sqs_record(click_message)
        ])

        self.assertEqual(result["batchItemFailures"], [])
        late_handler.late_table.update_item.assert_called()
        late_handler.mailbox_table.put_item.assert_not_called()

    def test_mixed_sqs_batch(self):
        click_message = {
            "event": {
                "event_id": "click-002",
                "user_id": "user-002",
                "event_type": "page_view",
                "event_time": "2026-11-17T14:30:00Z",
            },
            "window_start": "2026-11-17T14:30:00+00:00",
            "lateness_seconds": 50,
        }

        result = self.invoke([
            self.sqs_record(self.email_message, "email-sqs"),
            self.sqs_record(click_message, "click-sqs"),
        ])

        self.assertEqual(result["batchItemFailures"], [])
        late_handler.mailbox_table.put_item.assert_called_once()
        late_handler.late_table.update_item.assert_called()


if __name__ == "__main__":
    unittest.main()
