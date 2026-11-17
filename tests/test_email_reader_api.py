import importlib.util
import json
import os
import sys
import unittest
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

from botocore.exceptions import ClientError

ROOT = Path(__file__).resolve().parents[1]
READER_DIR = ROOT / "lambdas" / "email_reader"
sys.path.insert(0, str(READER_DIR))

with patch.dict(
    os.environ,
    {"EMAIL_MAILBOX_TABLE": "test-mailboxes"},
):
    with patch("boto3.resource"):
        spec = importlib.util.spec_from_file_location(
            "week8_email_reader_under_test",
            READER_DIR / "handler.py",
        )
        reader = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(reader)


class EmailReaderApiTests(unittest.TestCase):

    def setUp(self):
        self.event = {
            "httpMethod": "GET",
            "pathParameters": {
                "recipient": "BOB@example.com"
            },
            "queryStringParameters": None,
        }

    def invoke(self, event=None):
        result = reader.lambda_handler(
            event or self.event,
            None,
        )
        return result["statusCode"], json.loads(result["body"])

    def test_inbox_endpoint(self):
        messages = [
            {
                "message_id": "m1",
                "subject": "Meeting",
                "delivery_latency_ms": Decimal("25.5"),
            }
        ]

        with patch.object(
            reader,
            "list_messages",
            return_value=messages,
        ) as query:
            status, body = self.invoke()

        self.assertEqual(status, 200)
        self.assertEqual(body["recipient"], "bob@example.com")
        self.assertEqual(body["returned_count"], 1)
        self.assertEqual(
            body["messages"][0]["delivery_latency_ms"],
            25.5,
        )
        self.assertEqual(
            query.call_args.kwargs["folder"],
            "INBOX",
        )

    def test_search_endpoint(self):
        self.event["queryStringParameters"] = {
            "folder": "ALL",
            "q": "invoice",
            "start": "2026-11-17",
            "end": "2026-11-18",
            "limit": "500",
            "order": "asc",
        }

        with patch.object(
            reader,
            "list_messages",
            return_value=[],
        ) as query:
            status, body = self.invoke()

        self.assertEqual(status, 200)
        self.assertEqual(body["returned_count"], 0)

        args = query.call_args.kwargs
        self.assertEqual(args["keyword"], "invoice")
        self.assertEqual(args["folder"], "ALL")
        self.assertEqual(args["start"], "2026-11-17")
        self.assertEqual(args["end"], "2026-11-18")
        self.assertEqual(args["limit"], 500)
        self.assertFalse(args["descending"])

    def test_message_viewer(self):
        self.event["pathParameters"]["message_id"] = "m1"

        with patch.object(
            reader,
            "get_message",
            return_value={
                "message_id": "m1",
                "subject": "Hello",
                "body": "Full message content",
            },
        ):
            status, body = self.invoke()

        self.assertEqual(status, 200)
        self.assertEqual(body["message"]["message_id"], "m1")

    def test_missing_message_returns_404(self):
        self.event["pathParameters"]["message_id"] = "missing"

        with patch.object(
            reader,
            "get_message",
            return_value=None,
        ):
            status, body = self.invoke()

        self.assertEqual(status, 404)
        self.assertEqual(body["error"], "Message not found")

    def test_invalid_limit_returns_400(self):
        self.event["queryStringParameters"] = {
            "limit": "not-a-number"
        }

        status, body = self.invoke()

        self.assertEqual(status, 400)
        self.assertIn("error", body)

    def test_missing_recipient_returns_400(self):
        self.event["pathParameters"] = {}

        status, body = self.invoke()

        self.assertEqual(status, 400)
        self.assertEqual(body["error"], "Recipient required")

    def test_post_method_rejected(self):
        self.event["httpMethod"] = "POST"

        status, body = self.invoke()

        self.assertEqual(status, 405)

    def test_database_failure_returns_503(self):
        error = ClientError(
            {
                "Error": {
                    "Code": "ServiceUnavailable",
                    "Message": "Database unavailable",
                }
            },
            "Query",
        )

        with patch.object(
            reader,
            "list_messages",
            side_effect=error,
        ):
            with patch.object(reader, "logger"):
                status, body = self.invoke()

        self.assertEqual(status, 503)
        self.assertEqual(
            body["error"],
            "Mailbox temporarily unavailable",
        )


if __name__ == "__main__":
    unittest.main()
