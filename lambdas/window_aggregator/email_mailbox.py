"""Idempotent email mailbox storage."""

from datetime import datetime, timezone

from botocore.exceptions import ClientError


def save_email(email, table, folder="INBOX", processed_at=None):
    if folder not in ("INBOX", "LATE"):
        raise ValueError("Unsupported folder")

    event_time = datetime.fromisoformat(
        email["event_time"].replace("Z", "+00:00")
    )
    ingested_at = datetime.fromisoformat(
        email["ingested_at"].replace("Z", "+00:00")
    )

    if event_time.tzinfo is None or ingested_at.tzinfo is None:
        raise ValueError("Email timestamps must have timezones")

    event_time = event_time.astimezone(timezone.utc)
    ingested_at = ingested_at.astimezone(timezone.utc)
    processed_at = processed_at or datetime.now(timezone.utc)

    recipient = email["recipient"].strip().lower()
    message_id = str(email["message_id"])

    item = {
        "recipient": recipient,
        "message_id": message_id,
        "sender": email["sender"],
        "subject": email["subject"],
        "body": email.get("body", ""),
        "date": email.get("date", email["event_time"]),
        "event_time": event_time.isoformat(),
        "event_time_id": (
            event_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            + "#" + message_id
        ),
        "ingested_at": ingested_at.isoformat(),
        "delivered_at": processed_at.isoformat(),
        "delivery_latency_ms": max(
            0,
            int((processed_at - ingested_at).total_seconds() * 1000),
        ),
        "folder": folder,
    }

    try:
        table.put_item(
            Item=item,
            ConditionExpression=(
                "attribute_not_exists(recipient) "
                "AND attribute_not_exists(message_id)"
            ),
        )
    except ClientError as exc:
        if exc.response["Error"]["Code"] == (
            "ConditionalCheckFailedException"
        ):
            return False
        raise

    return True
