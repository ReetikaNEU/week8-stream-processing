import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lambdas" / "email_reader"))

from mailbox_queries import (
    _date_bound,
    get_message,
    list_messages,
)


class EmailReaderTests(unittest.TestCase):

    def setUp(self):
        self.table = MagicMock()

        self.invoice = {
            "recipient": "bob@example.com",
            "message_id": "m1",
            "subject": "Invoice November",
            "body": "Payment due",
            "folder": "INBOX",
        }

        self.meeting = {
            "recipient": "bob@example.com",
            "message_id": "m2",
            "subject": "Team meeting",
            "body": "Tomorrow morning",
            "folder": "INBOX",
        }

        self.late_invoice = {
            "recipient": "bob@example.com",
            "message_id": "m3",
            "subject": "Late invoice",
            "body": "Old payment",
            "folder": "LATE",
        }

    def test_inbox_uses_newest_first_index(self):
        self.table.query.return_value = {
            "Items": [self.meeting, self.invoice]
        }

        result = list_messages(
            self.table, "BOB@example.com"
        )

        self.assertEqual(
            [m["message_id"] for m in result],
            ["m2", "m1"],
        )

        params = self.table.query.call_args.kwargs

        self.assertEqual(
            params["IndexName"],
            "RecipientByDate",
        )
        self.assertFalse(params["ScanIndexForward"])

    def test_invoice_folder_excludes_late(self):
        self.table.query.return_value = {
            "Items": [
                self.invoice,
                self.meeting,
                self.late_invoice,
            ]
        }

        result = list_messages(
            self.table,
            "bob@example.com",
            folder="INVOICE",
        )

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["message_id"], "m1")

    def test_late_folder(self):
        self.table.query.return_value = {
            "Items": [self.invoice, self.late_invoice]
        }

        result = list_messages(
            self.table,
            "bob@example.com",
            folder="LATE",
        )

        self.assertEqual(
            [m["message_id"] for m in result],
            ["m3"],
        )

    def test_dino_subject_rule(self):
        messages = [
            {
                "subject": "Hello Dino",
                "folder": "INBOX",
                "message_id": "d1",
            },
            {
                "subject": "Systems Design in the Age of AI",
                "folder": "INBOX",
                "message_id": "d2",
            },
            {
                "subject": "Unrelated topic",
                "folder": "INBOX",
                "message_id": "d3",
            },
        ]

        self.table.query.return_value = {
            "Items": messages
        }

        result = list_messages(
            self.table,
            "bob@example.com",
            folder="DINO",
        )

        self.assertEqual(
            [m["message_id"] for m in result],
            ["d1", "d2"],
        )

    def test_keyword_search_case_insensitive(self):
        body_match = dict(self.meeting)
        body_match["body"] = "Please review the INVOICE"

        self.table.query.return_value = {
            "Items": [
                self.invoice,
                body_match,
                self.late_invoice,
            ]
        }

        result = list_messages(
            self.table,
            "bob@example.com",
            folder="ALL",
            keyword="invoice",
        )

        self.assertEqual(len(result), 3)

    def test_pagination_does_not_lose_matches(self):
        self.table.query.side_effect = [
            {
                "Items": [self.meeting],
                "LastEvaluatedKey": {
                    "recipient": "bob@example.com",
                    "message_id": "m2",
                },
            },
            {
                "Items": [self.invoice],
            },
        ]

        result = list_messages(
            self.table,
            "bob@example.com",
            folder="ALL",
            keyword="invoice",
        )

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["message_id"], "m1")
        self.assertEqual(self.table.query.call_count, 2)

        second_call = self.table.query.call_args_list[1]
        self.assertIn(
            "ExclusiveStartKey",
            second_call.kwargs,
        )

    def test_date_bound_normalizes_timezone(self):
        result = _date_bound(
            "2026-11-17T09:30:00-05:00"
        )

        self.assertEqual(
            result,
            "2026-11-17T14:30:00.000000Z#",
        )

    def test_reversed_date_range_rejected(self):
        with self.assertRaises(ValueError):
            list_messages(
                self.table,
                "bob@example.com",
                start="2026-11-18",
                end="2026-11-17",
            )

        self.table.query.assert_not_called()

    def test_single_message_viewer(self):
        self.table.get_item.return_value = {
            "Item": self.invoice
        }

        result = get_message(
            self.table,
            "BOB@example.com",
            "m1",
        )

        self.assertEqual(
            result["subject"],
            "Invoice November",
        )

        self.table.get_item.assert_called_once_with(
            Key={
                "recipient": "bob@example.com",
                "message_id": "m1",
            },
            ConsistentRead=True,
        )


if __name__ == "__main__":
    unittest.main()
