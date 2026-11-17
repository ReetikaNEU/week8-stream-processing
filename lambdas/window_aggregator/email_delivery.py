"""Email delivery processing using event-time watermarks."""

import json
from datetime import datetime, timedelta, timezone

from botocore.exceptions import ClientError

from email_mailbox import save_email


def parse_time(value):
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None:
        raise ValueError("Timezone required")
    return result.astimezone(timezone.utc)


def process_email_batch(
    shard_id,
    emails,
    watermark_table,
    mailbox_table,
    sqs_client,
    late_queue_url,
    window_seconds=60,
    grace_seconds=30,
):
    state_key = "email#" + shard_id

    state = watermark_table.get_item(
        Key={"shard_id": state_key}
    ).get("Item", {})

    watermark = (
        parse_time(state["watermark"])
        if state.get("watermark")
        else None
    )

    counts = {
        "delivered": 0,
        "duplicate": 0,
        "late_queued": 0,
    }

    for email in emails:
        event_time = parse_time(email["event_time"])

        window_start = datetime.fromtimestamp(
            (int(event_time.timestamp()) // window_seconds)
            * window_seconds,
            tz=timezone.utc,
        )

        window_end = window_start + timedelta(
            seconds=window_seconds
        )
        close_time = window_end + timedelta(
            seconds=grace_seconds
        )

        is_late = (
            watermark is not None
            and watermark >= close_time
        )

        if is_late:
            public_email = {
                key: value
                for key, value in email.items()
                if not key.startswith("_")
            }

            sqs_client.send_message(
                QueueUrl=late_queue_url,
                MessageBody=json.dumps({
                    "record_type": "email",
                    "event": public_email,
                    "window_start": window_start.isoformat(),
                    "lateness_seconds": (
                        watermark - close_time
                    ).total_seconds(),
                }),
            )
            counts["late_queued"] += 1
        else:
            inserted = save_email(email, mailbox_table)
            if inserted:
                counts["delivered"] += 1
            else:
                counts["duplicate"] += 1

        if watermark is None or event_time > watermark:
            watermark = event_time

    if watermark is not None:
        try:
            watermark_table.update_item(
                Key={"shard_id": state_key},
                UpdateExpression="SET #wm = :wm",
                ConditionExpression=(
                    "attribute_not_exists(#wm) OR #wm < :wm"
                ),
                ExpressionAttributeNames={
                    "#wm": "watermark"
                },
                ExpressionAttributeValues={
                    ":wm": watermark.isoformat(
                        timespec="microseconds"
                    )
                },
            )
        except ClientError as exc:
            if exc.response["Error"]["Code"] != (
                "ConditionalCheckFailedException"
            ):
                raise

    return counts
