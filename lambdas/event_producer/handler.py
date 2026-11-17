"""
EventProducer Lambda
=====================
Triggered by: API Gateway POST /events
Does:
  1. Validates the event payload (including the timestamp - see below)
  2. Stamps `ingested_at` (processing time - when the server received the event)
  3. Writes to Kinesis using `user_id` as the partition key

CRITICAL DESIGN POINT - event_time vs ingested_at:
  The CALLER supplies `event_time` (when the click actually happened on
  the user's device). This Lambda adds `ingested_at` (server-side receive
  time). These are usually close together, but can diverge significantly:
    - Mobile app buffers events offline, sends a batch when reconnected
    - Client clock skew
    - Network delay

  WindowAggregator buckets events by event_time, NOT ingested_at.
  This is what makes "late data" possible - an event can arrive at the
  server (ingested_at = now) but belong to a window that closed minutes
  ago (event_time = 5 minutes ago).

THE TIMESTAMP IS THE ONE FIELD THE WATERMARK TRUSTS:
  The aggregator moves a per-shard watermark up to the largest event_time it
  has seen, and the watermark never moves backwards. One bad timestamp can
  therefore do lasting damage:
    - a timestamp WITHOUT a timezone cannot be compared with the others and
      breaks the aggregator;
    - a timestamp far in the FUTURE (a wrong device clock) drags the watermark
      ahead, and every normal event after it is classified late.
  So this handler rejects both at the edge (HTTP 400), and it stores
  event_time normalized to UTC. Never trust a client clock blindly.

PARTITION KEY = user_id:
  All events for one user go to the same shard, in order. This means
  per-user event sequences are preserved. But there is NO ordering
  guarantee ACROSS users on different shards - this is why the aggregator
  keeps a watermark per shard. (A production engine would combine the
  per-shard watermarks, typically by taking the minimum; this lab keeps
  them separate to stay simple.)
"""
import json
import logging
import os
import uuid
from datetime import datetime, timedelta, timezone

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

kinesis_client = boto3.client("kinesis")
CLICKSTREAM_NAME = os.environ["CLICKSTREAM_NAME"]

REQUIRED_FIELDS = {"user_id", "event_type"}
VALID_EVENT_TYPES = {"page_view", "click", "purchase", "scroll"}

# Kinesis limits a partition key to 256 characters.
MAX_USER_ID_LENGTH = 256
# How far ahead of the server clock an event_time may be before we call it a
# bad clock and reject it. Small positive skew between devices is normal.
MAX_FUTURE_SKEW_SECONDS = int(os.getenv("MAX_FUTURE_SKEW_SECONDS", "60"))


def lambda_handler(event, context):
    try:
        body = json.loads(event.get("body") or "{}")
    except json.JSONDecodeError:
        return _response(400, {"error": "Request body is not valid JSON"})

    if not isinstance(body, dict):
        return _response(400, {"error": "Request body must be a JSON object"})

    now = datetime.now(timezone.utc)

    errors, event_time = _validate(body, now)
    if errors:
        return _response(400, {"error": "Validation failed", "details": errors})

    # event_time: caller-supplied (when the click happened on the device),
    # already checked and converted to UTC. Defaults to now if not provided
    # (most lab traffic will not provide it, making event_time == ingested_at
    # for the baseline - see Build phase).
    event_time = event_time or now

    record = {
        "event_id":    str(uuid.uuid4()),
        "user_id":     body["user_id"],
        "event_type":  body["event_type"],
        "page":        body.get("page", "/"),
        "event_time":  event_time.isoformat(),  # WHEN IT HAPPENED - windowing key (UTC)
        "ingested_at": now.isoformat(),         # WHEN SERVER RECEIVED IT
    }

    try:
        kinesis_client.put_record(
            StreamName=CLICKSTREAM_NAME,
            Data=json.dumps(record).encode(),
            PartitionKey=body["user_id"],   # per-user ordering, not global
        )
    except ClientError as e:
        logger.error("kinesis_put_record_failed", extra={"error": str(e)})
        return _response(500, {"error": "Failed to write event"})

    return _response(202, {
        "event_id":   record["event_id"],
        "event_time": record["event_time"],
        "status":     "accepted",
    })


def _validate(body: dict, now: datetime):
    """Return (errors, event_time). event_time is a timezone-aware UTC datetime,
    or None when the caller did not supply one."""
    missing = REQUIRED_FIELDS - set(body.keys())
    if missing:
        return [f"Missing required fields: {sorted(missing)}"], None

    errors = []

    user_id = body["user_id"]
    if not isinstance(user_id, str) or not user_id or len(user_id) > MAX_USER_ID_LENGTH:
        errors.append(f"user_id must be a non-empty string of at most {MAX_USER_ID_LENGTH} characters")

    event_type = body["event_type"]
    if not isinstance(event_type, str) or event_type not in VALID_EVENT_TYPES:
        errors.append(f"event_type must be one of {sorted(VALID_EVENT_TYPES)}")

    event_time = None
    if "event_time" in body:
        event_time, problem = _parse_event_time(body["event_time"], now)
        if problem:
            errors.append(problem)

    return errors, event_time


def _parse_event_time(value, now: datetime):
    """Return (utc_datetime, None) or (None, problem_text)."""
    if not isinstance(value, str):
        return None, "event_time must be an ISO 8601 string"
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None, "event_time must be ISO 8601 format"

    # A timestamp with no timezone is ambiguous (whose local time?) and cannot be
    # compared with the aware timestamps the aggregator works with.
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None, ("event_time must include a timezone, "
                      "for example 2026-11-07T12:00:00Z or 2026-11-07T12:00:00+00:00")

    parsed = parsed.astimezone(timezone.utc)

    if parsed > now + timedelta(seconds=MAX_FUTURE_SKEW_SECONDS):
        return None, (f"event_time is more than {MAX_FUTURE_SKEW_SECONDS} seconds in the "
                      f"future; check the sender's clock")

    return parsed, None


def _response(status_code: int, body: dict) -> dict:
    return {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body),
    }
