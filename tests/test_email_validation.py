import sys
import unittest
from pathlib import Path
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime

sys.path.insert(
    0,
    str(Path(__file__).resolve().parents[1]
        / "lambdas" / "event_producer")
)

from email_validation import validate_email


class EmailValidationTests(unittest.TestCase):

    def setUp(self):
        self.now = datetime.now(timezone.utc)
        self.email = {
            "message_id": "msg-001",
            "sender": "alice@example.com",
            "recipient": "BOB@example.com",
            "subject": "Project meeting",
            "body": "Meeting tomorrow",
            "date": (
                self.now - timedelta(minutes=1)
            ).isoformat()
        }

    def test_valid_email(self):
        result, errors = validate_email(self.email, self.now)
        self.assertEqual(errors, [])
        self.assertEqual(result["recipient"], "bob@example.com")

    def test_rfc_date(self):
        self.email["date"] = format_datetime(self.now)
        result, errors = validate_email(self.email, self.now)
        self.assertEqual(errors, [])

    def test_missing_timezone(self):
        self.email["date"] = self.now.replace(
            tzinfo=None
        ).isoformat()
        result, errors = validate_email(self.email, self.now)
        self.assertIsNone(result)
        self.assertTrue(errors)

    def test_future_date(self):
        self.email["date"] = (
            self.now + timedelta(hours=2)
        ).isoformat()
        result, errors = validate_email(self.email, self.now)
        self.assertIsNone(result)
        self.assertTrue(errors)

    def test_invalid_address(self):
        self.email["recipient"] = "not-an-email"
        result, errors = validate_email(self.email, self.now)
        self.assertIsNone(result)
        self.assertTrue(errors)

    def test_missing_message_id(self):
        del self.email["message_id"]
        result, errors = validate_email(self.email, self.now)
        self.assertIsNone(result)
        self.assertTrue(errors)


if __name__ == "__main__":
    unittest.main()
