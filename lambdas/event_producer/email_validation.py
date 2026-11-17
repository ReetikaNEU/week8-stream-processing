from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime

REQUIRED_FIELDS = (
    "message_id",
    "sender",
    "recipient",
    "subject",
    "date",
)

def validate_email(payload, now=None):
    now = now or datetime.now(timezone.utc)

    if not isinstance(payload, dict):
        return None, ["Email must be a JSON object"]

    errors = []

    for field in REQUIRED_FIELDS:
        value = payload.get(field)
        if not isinstance(value, str) or not value.strip():
            errors.append(f"Missing or invalid {field}")

    if errors:
        return None, errors

    try:
        date_value = payload["date"]

        try:
            event_time = datetime.fromisoformat(
                date_value.replace("Z", "+00:00")
            )
        except ValueError:
            event_time = parsedate_to_datetime(date_value)

        if event_time.tzinfo is None:
            return None, ["Email date has no timezone"]

        event_time = event_time.astimezone(timezone.utc)

    except (ValueError, TypeError, IndexError):
        return None, ["Invalid email date"]

    if event_time > now + timedelta(seconds=60):
        return None, ["Email date is too far in the future"]

    if "@" not in payload["sender"] or "@" not in payload["recipient"]:
        return None, ["Invalid sender or recipient address"]

    if not isinstance(payload.get("body", ""), str):
        return None, ["Email body must be a string"]

    normalized = dict(payload)
    normalized["event_time"] = event_time.isoformat()
    normalized["ingested_at"] = now.isoformat()
    normalized["recipient"] = payload["recipient"].strip().lower()

    return normalized, []
