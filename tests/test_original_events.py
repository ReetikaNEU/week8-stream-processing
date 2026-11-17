import json
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lambdas" / "event_producer"))

os.environ.setdefault("CLICKSTREAM_NAME", "test-stream")

with patch("boto3.client"):
    import handler as producer


class OriginalEventTests(unittest.TestCase):

    def test_valid_event(self):
        event = {
            "path": "/events",
            "body": json.dumps({
                "user_id": "user-001",
                "event_type": "page_view"
            })
        }

        with patch.object(producer, "kinesis_client") as mock:
            response = producer.lambda_handler(event, None)

            self.assertEqual(response["statusCode"], 202)
            mock.put_record.assert_called_once()

            arguments = mock.put_record.call_args.kwargs
            self.assertEqual(arguments["PartitionKey"], "user-001")

    def test_invalid_event(self):
        event = {
            "path": "/events",
            "body": json.dumps({"event_type": "click"})
        }

        response = producer.lambda_handler(event, None)
        self.assertEqual(response["statusCode"], 400)


if __name__ == "__main__":
    unittest.main()
