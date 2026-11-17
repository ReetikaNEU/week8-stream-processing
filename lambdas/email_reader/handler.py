"""Read-only HTTP API for email mailboxes."""

import json
import logging
import os
from decimal import Decimal

import boto3
from botocore.exceptions import ClientError

from mailbox_queries import get_message, list_messages


logger = logging.getLogger(__name__)

mailbox_table = boto3.resource("dynamodb").Table(
    os.environ["EMAIL_MAILBOX_TABLE"]
)


def json_default(value):
    if isinstance(value, Decimal):
        if value == value.to_integral_value():
            return int(value)
        return float(value)

    raise TypeError(
        f"Object of type {type(value).__name__} is not JSON serializable"
    )


def response(status, payload):
    return {
        "statusCode": status,
        "headers": {
            "Content-Type": "application/json",
            "Cache-Control": "no-store",
        },
        "body": json.dumps(payload, default=json_default),
    }


def lambda_handler(event, context):
    if event.get("httpMethod") != "GET":
        return response(405, {"error": "Method not allowed"})

    params = event.get("pathParameters") or {}
    query = event.get("queryStringParameters") or {}

    recipient = params.get("recipient")
    message_id = params.get("message_id")

    if not recipient:
        return response(400, {"error": "Recipient required"})

    try:
        # Open a single message.
        if message_id:
            message = get_message(
                mailbox_table,
                recipient,
                message_id,
            )

            if message is None:
                return response(404, {"error": "Message not found"})

            return response(200, {"message": message})

        # List inbox, folder, or keyword/date search.
        folder = query.get("folder") or "INBOX"
        keyword = query.get("q") or query.get("keyword")
        start = query.get("start")
        end = query.get("end")
        limit = int(query.get("limit") or 1000)

        descending = (
            (query.get("order") or "desc").lower() != "asc"
        )

        messages = list_messages(
            mailbox_table,
            recipient=recipient,
            folder=folder,
            keyword=keyword,
            start=start,
            end=end,
            limit=limit,
            descending=descending,
        )

        return response(200, {
            "recipient": recipient.strip().lower(),
            "folder": folder,
            "returned_count": len(messages),
            "messages": messages,
        })

    except (ValueError, TypeError) as exc:
        return response(400, {"error": str(exc)})

    except ClientError:
        logger.exception("Mailbox DynamoDB operation failed")
        return response(
            503,
            {"error": "Mailbox temporarily unavailable"},
        )
