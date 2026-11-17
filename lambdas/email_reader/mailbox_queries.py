"""Mailbox viewing, folder filtering, and search."""

from datetime import date, datetime, time, timezone

from boto3.dynamodb.conditions import Key


FOLDER_RULES = {
    "INVOICE": ("invoice",),
    "MEETING": ("meeting",),
    "NEWSLETTER": ("newsletter",),
    "DINO": ("dino", "systems design in the age of ai"),
}


def _date_bound(value, upper=False):
    if not value:
        return None

    if len(value) == 10:
        day = date.fromisoformat(value)
        parsed = datetime.combine(
            day,
            time.max if upper else time.min,
            tzinfo=timezone.utc,
        )
    else:
        parsed = datetime.fromisoformat(
            value.replace("Z", "+00:00")
        )
        if parsed.tzinfo is None:
            raise ValueError("Search timestamps require timezones")
        parsed = parsed.astimezone(timezone.utc)

    prefix = parsed.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    return prefix + ("#\uffff" if upper else "#")


def _folder_matches(item, folder):
    folder = folder.strip().upper()
    stored = item.get("folder", "INBOX").upper()

    if folder == "ALL":
        return True

    if folder in ("INBOX", "LATE"):
        return stored == folder

    # Subject-rule folders contain delivered mail only.
    if stored != "INBOX":
        return False

    subject = item.get("subject", "").casefold()

    if folder in FOLDER_RULES:
        return any(
            word in subject
            for word in FOLDER_RULES[folder]
        )

    if folder.startswith("SUBJECT:"):
        keyword = folder.split(":", 1)[1].strip().casefold()
        return bool(keyword) and keyword in subject

    raise ValueError("Unknown folder")


def list_messages(
    table,
    recipient,
    folder="INBOX",
    keyword=None,
    start=None,
    end=None,
    limit=1000,
    descending=True,
):
    if not recipient or "@" not in recipient:
        raise ValueError("Invalid recipient")

    if not isinstance(limit, int) or not 1 <= limit <= 10000:
        raise ValueError("Limit must be between 1 and 10000")

    recipient = recipient.strip().lower()

    lower = _date_bound(start)
    upper = _date_bound(end, upper=True)

    if lower and upper and lower > upper:
        raise ValueError("Start date must not exceed end date")

    condition = Key("recipient").eq(recipient)

    if lower and upper:
        condition &= Key("event_time_id").between(lower, upper)
    elif lower:
        condition &= Key("event_time_id").gte(lower)
    elif upper:
        condition &= Key("event_time_id").lte(upper)

    params = {
        "IndexName": "RecipientByDate",
        "KeyConditionExpression": condition,
        "ScanIndexForward": not descending,
    }

    matches = []
    search = keyword.casefold() if keyword else None

    while True:
        result = table.query(**params)

        for item in result.get("Items", []):
            if not _folder_matches(item, folder):
                continue

            if search:
                searchable = (
                    item.get("subject", "")
                    + " "
                    + item.get("body", "")
                ).casefold()

                if search not in searchable:
                    continue

            matches.append(item)

            if len(matches) >= limit:
                return matches

        next_key = result.get("LastEvaluatedKey")

        if not next_key:
            break

        params["ExclusiveStartKey"] = next_key

    return matches


def get_message(table, recipient, message_id):
    if not recipient or not message_id:
        raise ValueError("Recipient and Message-ID required")

    result = table.get_item(
        Key={
            "recipient": recipient.strip().lower(),
            "message_id": message_id,
        },
        ConsistentRead=True,
    )

    return result.get("Item")
