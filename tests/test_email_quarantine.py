import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock

from botocore.exceptions import ClientError

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(
    0, str(ROOT / "lambdas" / "event_producer")
)

from email_quarantine import quarantine_email


class QuarantineTests(unittest.TestCase):

    def setUp(self):
        self.table = MagicMock()
        self.payload = {
            "message_id": "bad-001",
            "date": "invalid-date"
        }

    def test_quarantine_record_created(self):
        quarantine_id = quarantine_email(
            self.payload,
            ["Invalid email date"],
            self.table
        )

        self.table.put_item.assert_called_once()

        item = self.table.put_item.call_args.kwargs["Item"]

        self.assertEqual(item["status"], "QUARANTINED")
        self.assertEqual(
            item["quarantine_id"], quarantine_id
        )

    def test_duplicate_id_is_stable(self):
        first = quarantine_email(
            self.payload, ["Invalid date"], self.table
        )
        second = quarantine_email(
            self.payload, ["Invalid date"], self.table
        )

        self.assertEqual(first, second)

    def test_existing_record_is_handled(self):
        self.table.put_item.side_effect = ClientError(
            {
                "Error": {
                    "Code": "ConditionalCheckFailedException",
                    "Message": "Already exists"
                }
            },
            "PutItem"
        )

        result = quarantine_email(
            self.payload,
            ["Invalid date"],
            self.table
        )

        self.assertIsInstance(result, str)

    def test_unexpected_aws_error_is_raised(self):
        self.table.put_item.side_effect = ClientError(
            {
                "Error": {
                    "Code": "AccessDeniedException",
                    "Message": "Denied"
                }
            },
            "PutItem"
        )

        with self.assertRaises(ClientError):
            quarantine_email(
                self.payload,
                ["Invalid date"],
                self.table
            )


if __name__ == "__main__":
    unittest.main()
