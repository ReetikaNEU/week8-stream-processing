import json
from botocore.exceptions import ClientError
from email_validation import validate_email
from email_quarantine import quarantine_email


def response(status, body):
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body),
    }


def handle_email(event, kinesis_client, stream_name,
                 quarantine_table=None):
    raw_body = event.get("body") or "{}"

    def reject(reasons, message):
        if quarantine_table is not None:
            try:
                quarantine_email(
                    raw_body, reasons, quarantine_table
                )
            except ClientError:
                return response(503, {
                    "error": "Quarantine storage failed"
                })

        return response(400, {
            "error": message,
            "details": reasons,
        })

    try:
        body = json.loads(raw_body)
    except (ValueError, TypeError):
        return reject(["Invalid JSON"], "Invalid JSON")

    email, errors = validate_email(body)

    if errors:
        return reject(errors, "Email validation failed")

    if len(email["recipient"].encode("utf-8")) > 256:
        return response(400, {
            "error": "Recipient exceeds partition-key limit"
        })

    record = {
        "record_type": "email",
        "message_id": email["message_id"],
        "sender": email["sender"],
        "recipient": email["recipient"],
        "subject": email["subject"],
        "body": email.get("body", ""),
        "date": email["date"],
        "event_time": email["event_time"],
        "ingested_at": email["ingested_at"],
    }

    try:
        kinesis_client.put_record(
            StreamName=stream_name,
            PartitionKey=email["recipient"],
            Data=json.dumps(record).encode("utf-8"),
        )
    except ClientError:
        return response(500, {
            "error": "Email ingestion failed"
        })

    return response(202, {
        "message_id": email["message_id"],
        "status": "accepted",
    })
