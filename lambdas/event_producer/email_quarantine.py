import hashlib
import json
from datetime import datetime, timezone

from botocore.exceptions import ClientError


def quarantine_email(raw_payload, reasons, table, now=None):
    now = now or datetime.now(timezone.utc)

    try:
        payload = json.loads(raw_payload) if isinstance(
            raw_payload, str
        ) else raw_payload
    except (ValueError, TypeError):
        payload = raw_payload

    canonical = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        default=str
    )

    quarantine_id = hashlib.sha256(
        canonical.encode("utf-8")
    ).hexdigest()

    item = {
        "quarantine_id": quarantine_id,
        "received_at": now.isoformat(),
        "raw_payload": canonical,
        "reasons": list(reasons),
        "status": "QUARANTINED"
    }

    try:
        table.put_item(
            Item=item,
            ConditionExpression=(
                "attribute_not_exists(quarantine_id)"
            )
        )
    except ClientError as exc:
        if exc.response["Error"]["Code"] != (
            "ConditionalCheckFailedException"
        ):
            raise

    return quarantine_id
