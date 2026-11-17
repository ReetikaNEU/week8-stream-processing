"""
WindowAggregator Lambda
========================
Triggered by: Kinesis (batch_size=100, max_batching_window=10s)

WINDOWING MODEL: Tumbling windows, 60 seconds, non-overlapping.
  Window for event_time T is: [floor(T, 60s), floor(T, 60s) + 60s)
  e.g. event_time 14:32:47 belongs to window [14:32:00, 14:33:00)

WATERMARK MODEL:
  Each Kinesis shard is processed independently by this Lambda (Kinesis
  guarantees ordering WITHIN a shard, not across shards). For each shard,
  we track the maximum event_time seen so far - this is the "watermark"
  for that shard, persisted in DynamoDB since Lambda is stateless.

  A window [W_start, W_end) is considered CLOSED once:
      watermark >= W_end + GRACE_PERIOD_SECONDS

  Events with event_time falling in a CLOSED window are "late" - they
  are sent to the late-events SQS queue instead of updating
  window_aggregates (which is considered final/immutable once closed).

  Events with event_time falling in an OPEN window update
  window_aggregates normally - even if they arrive "out of order"
  relative to other events in the same batch.

WHEN EXACTLY IS AN EVENT LATE?
  Not "when it is older than 90 seconds". Let L be the watermark minus the
  event's own time, and o how far into its window the event falls (0 to 59 s).
  The event's window closes at (event_time - o + 90 s), so the event is late
  exactly when  L >= 90 - o.
    - L under 30 s (the grace period): never late.
    - L of 90 s or more: always late.
    - in between: it depends on where in its window the event happened.
  The grace period is the guaranteed on-time budget; 90 s is only the worst
  case for an event stamped at the very start of its window.

WHY THIS MATTERS:
  Without a watermark, "is this window done?" has no answer - you would
  either (a) never close a window (unbounded state, unbounded memory) or
  (b) close windows on a wall-clock timer (processing time), which is
  wrong when event_time and ingested_at diverge (mobile offline buffering,
  clock skew, network delay).

  The watermark says: "I am confident no more events with event_time
  before this point will arrive - except in pathological cases, which
  go to the late-events path."

THIS IS A SIMPLIFIED WATERMARK:
  Production systems (Flink, Kinesis Data Analytics) use more
  sophisticated watermark propagation across parallel workers, typically
  taking the MINIMUM watermark across inputs. This per-shard,
  DynamoDB-persisted watermark is intentionally simple so students can SEE
  every step of the logic. The comments throughout this file are the lesson.

TWO KINDS OF ERROR, TWO KINDS OF HANDLING:
  - BAD DATA (not JSON, a missing field, a timestamp with no timezone, a
    timestamp far in the future): the record can never be processed. It is
    skipped and logged (`bad_record`) and the rest of the batch carries on.
    One poison record must not block a shard.
  - INFRASTRUCTURE errors (DynamoDB or SQS unavailable): the exception is
    raised, the whole batch is retried, and after the retry limit the batch
    details go to the stream-failures queue. Partial batch responses exist
    for Kinesis (ReportBatchItemFailures) but this lab does not enable them.

A TIMESTAMP FAR IN THE FUTURE WOULD DRAG THE WATERMARK:
  The watermark only moves forward, so a record whose event_time is ahead of
  the time Kinesis received it (by more than MAX_FUTURE_SKEW_SECONDS) is
  rejected as bad data. The producer rejects such events too; this is the
  second line of defense for anything written to the stream directly.

SCENARIO D (parallelization) - DELIBERATELY UNPROTECTED:
  The watermark is read at the start of a batch and written at the end with an
  unconditional put. With ParallelizationFactor > 1, two batches from the same
  shard run at the same time, and the slower one can overwrite the faster one's
  newer watermark with an older one. This handler does not prevent that (it is
  the lesson), but it makes it visible: when a write lowers the stored value it
  logs `watermark_regressed`. The production fix is a conditional write that
  only moves the watermark forward - a Defend-phase exercise.
"""
import base64
import json
import logging
import os
from collections import defaultdict
from datetime import datetime, timezone, timedelta

import boto3
from botocore.exceptions import ClientError
from email_delivery import process_email_batch

logger = logging.getLogger()
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

dynamodb   = boto3.resource("dynamodb")
sqs_client = boto3.client("sqs")

WINDOW_TABLE          = os.environ["WINDOW_TABLE"]
WATERMARK_TABLE       = os.environ["WATERMARK_TABLE"]
LATE_QUEUE_URL        = os.environ["LATE_QUEUE_URL"]
WINDOW_SIZE_SECONDS   = int(os.environ["WINDOW_SIZE_SECONDS"])
GRACE_PERIOD_SECONDS  = int(os.environ["GRACE_PERIOD_SECONDS"])
MAX_FUTURE_SKEW_SECONDS = int(os.getenv("MAX_FUTURE_SKEW_SECONDS", "60"))
EMAIL_MAILBOX_TABLE = os.getenv("EMAIL_MAILBOX_TABLE")

REQUIRED_FIELDS = ("event_id", "user_id", "event_type", "event_time")
EMAIL_REQUIRED_FIELDS = (
    "message_id", "sender", "recipient",
    "subject", "date", "event_time", "ingested_at",
)

window_table    = dynamodb.Table(WINDOW_TABLE)
watermark_table = dynamodb.Table(WATERMARK_TABLE)
mailbox_table = (
    dynamodb.Table(EMAIL_MAILBOX_TABLE)
    if EMAIL_MAILBOX_TABLE else None
)


class BadRecord(Exception):
    """A record that can never be processed: skip it, log it, keep going."""


def lambda_handler(event, context):
    """
    event["Records"] - each record has:
      - kinesis.data (base64-encoded JSON event)
      - kinesis.partitionKey
      - kinesis.sequenceNumber
      - kinesis.approximateArrivalTimestamp (when Kinesis received it)
      - eventID  (contains the shard ID)
    """
    # Group records by shard - watermark is tracked per shard
    records_by_shard: dict[str, list[dict]] = defaultdict(list)
    email_records_by_shard: dict[str, list[dict]] = defaultdict(list)
    bad_by_shard: dict[str, int] = defaultdict(int)

    for record in event["Records"]:
        shard_id = _extract_shard_id(record["eventID"])
        try:
            decoded = _decode_record(record)
            if decoded.get("record_type") == "email":
                email_records_by_shard[shard_id].append(decoded)
            else:
                records_by_shard[shard_id].append(decoded)
        except BadRecord as exc:
            bad_by_shard[shard_id] += 1
            logger.warning(
                "bad_record shard=%s sequence=%s reason=%s",
                shard_id, record["kinesis"].get("sequenceNumber"), exc,
                extra={"shard_id": shard_id,
                       "sequence_number": record["kinesis"].get("sequenceNumber"),
                       "reason": str(exc)})

    if email_records_by_shard and mailbox_table is None:
        raise RuntimeError("EMAIL_MAILBOX_TABLE is not configured")

    for shard_id in set(records_by_shard) | set(bad_by_shard):
        valid = records_by_shard.get(shard_id, [])
        bad = bad_by_shard.get(shard_id, 0)
        if valid:
            _process_shard_batch(shard_id, valid, bad)
        else:
            # Nothing usable in this batch: do not touch the watermark.
            logger.info("batch_processed shard=%s records=0 bad_records=%d", shard_id, bad,
                        extra={"shard_id": shard_id, "records": 0, "bad_records": bad})

    for shard_id, emails in email_records_by_shard.items():
        counts = process_email_batch(
            shard_id,
            emails,
            watermark_table,
            mailbox_table,
            sqs_client,
            LATE_QUEUE_URL,
            window_seconds=WINDOW_SIZE_SECONDS,
            grace_seconds=GRACE_PERIOD_SECONDS,
        )

        logger.info(
            "email_batch_processed shard=%s delivered=%d duplicate=%d late_queued=%d",
            shard_id,
            counts["delivered"],
            counts["duplicate"],
            counts["late_queued"],
        )

    # Only INFRASTRUCTURE errors are raised (they retry the whole batch). Bad data
    # was skipped above. The value below is ignored unless the event source mapping
    # enables ReportBatchItemFailures, which this lab does not.
    return {"batchItemFailures": []}


def _decode_record(record: dict) -> dict:
    """Parse one Kinesis record into a payload dict with a parsed UTC `_event_dt`.
    Raises BadRecord for anything that can never be processed."""
    try:
        payload = json.loads(base64.b64decode(record["kinesis"]["data"]))
    except (ValueError, KeyError) as exc:     # bad base64, bad UTF-8 and bad JSON are all ValueErrors
        raise BadRecord(f"not valid JSON: {exc}")

    if not isinstance(payload, dict):
        raise BadRecord("payload is not a JSON object")
    required_fields = (
        EMAIL_REQUIRED_FIELDS
        if payload.get("record_type") == "email"
        else REQUIRED_FIELDS
    )
    for field in required_fields:
        if field not in payload:
            raise BadRecord(f"missing field {field}")

    event_time = _parse_event_time(payload["event_time"])
    if payload.get("record_type") == "email":
        _parse_event_time(payload["ingested_at"])

    # Defense in depth against a bad clock: an event cannot honestly claim to have
    # happened after the moment Kinesis received it.
    arrival = record["kinesis"].get("approximateArrivalTimestamp")
    if arrival is not None:
        arrived_at = datetime.fromtimestamp(float(arrival), tz=timezone.utc)
        if event_time > arrived_at + timedelta(seconds=MAX_FUTURE_SKEW_SECONDS):
            raise BadRecord(f"event_time {event_time.isoformat()} is more than "
                            f"{MAX_FUTURE_SKEW_SECONDS}s after the record arrived")

    payload["_event_dt"] = event_time
    return payload


def _parse_event_time(value) -> datetime:
    """ISO 8601 with a timezone -> timezone-aware UTC datetime. Anything else is bad data."""
    if not isinstance(value, str):
        raise BadRecord("event_time is not a string")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise BadRecord(f"event_time is not ISO 8601: {value!r}")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise BadRecord(f"event_time has no timezone: {value!r}")
    return parsed.astimezone(timezone.utc)


def _public(record: dict) -> dict:
    """The record as it was sent, without our internal parsed fields."""
    return {k: v for k, v in record.items() if not k.startswith("_")}


def _process_shard_batch(shard_id: str, records: list[dict], bad_records: int = 0):
    # -- Step 1: load current watermark for this shard ----------------------
    watermark = _get_watermark(shard_id)

    # -- Step 2: compute new watermark candidate -----------------------------
    # Watermark = max(event_time) seen across this batch and all prior batches
    max_event_time_in_batch = max(r["_event_dt"] for r in records)
    new_watermark = max(watermark, max_event_time_in_batch) if watermark else max_event_time_in_batch

    # -- Step 3: bucket each record into its window, classify late vs on-time -
    on_time_aggregates: dict[tuple[str, str], dict] = defaultdict(lambda: {
        "page_views": 0, "clicks": 0, "purchases": 0, "unique_users": set(),
    })
    late_records = []

    for r in records:
        event_time   = r["_event_dt"]
        window_start = _floor_to_window(event_time)
        window_end   = window_start + timedelta(seconds=WINDOW_SIZE_SECONDS)

        # Is this window already closed, according to the watermark BEFORE
        # this batch was processed? Use the watermark we LOADED (step 1),
        # not the new one we're computing - a window closes based on what
        # we knew BEFORE this record arrived.
        window_close_threshold = window_end + timedelta(seconds=GRACE_PERIOD_SECONDS)

        if watermark and watermark >= window_close_threshold:
            # LATE: this window was already closed when this record arrived
            late_records.append({
                "event":        _public(r),
                "window_start": window_start.isoformat(),
                "window_end":   window_end.isoformat(),
                "watermark_at_arrival": watermark.isoformat(),
                "lateness_seconds": (watermark - window_close_threshold).total_seconds(),
            })
            logger.warning(
                "late_event event_id=%s window_start=%s watermark=%s",
                r["event_id"], window_start.isoformat(), watermark.isoformat(),
                extra={"event_id": r["event_id"],
                       "event_time": r["event_time"],
                       "window_start": window_start.isoformat(),
                       "watermark": watermark.isoformat()})
            continue

        # ON TIME: aggregate normally
        key = (window_start.isoformat(), "metrics")
        agg = on_time_aggregates[key]
        if r["event_type"] == "page_view":
            agg["page_views"] += 1
        elif r["event_type"] == "click":
            agg["clicks"] += 1
        elif r["event_type"] == "purchase":
            agg["purchases"] += 1
        agg["unique_users"].add(str(r["user_id"]))

    # -- Step 4: write on-time aggregates to DynamoDB (additive update) ------
    for (window_start, metric_name), agg in on_time_aggregates.items():
        _update_window_aggregate(window_start, metric_name, agg)

    # -- Step 5: send late records to SQS ------------------------------------
    for late in late_records:
        sqs_client.send_message(
            QueueUrl=LATE_QUEUE_URL,
            MessageBody=json.dumps(late),
        )

    # -- Step 6: persist new watermark ---------------------------------------
    _set_watermark(shard_id, new_watermark)

    logger.info(
        "batch_processed shard=%s records=%d on_time_windows=%d late_records=%d bad_records=%d watermark=%s",
        shard_id, len(records), len(on_time_aggregates), len(late_records), bad_records,
        new_watermark.isoformat(),
        extra={"shard_id": shard_id,
               "records": len(records),
               "on_time_windows": len(on_time_aggregates),
               "late_records": len(late_records),
               "bad_records": bad_records,
               "watermark": new_watermark.isoformat()})


def _floor_to_window(dt: datetime) -> datetime:
    """Round down to the nearest WINDOW_SIZE_SECONDS boundary."""
    epoch = int(dt.timestamp())
    floored = epoch - (epoch % WINDOW_SIZE_SECONDS)
    return datetime.fromtimestamp(floored, tz=timezone.utc)


def _parse_stored_watermark(text: str) -> datetime:
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        # Written by an older version of this handler that accepted timestamps
        # without a timezone: read it as UTC rather than crash.
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _get_watermark(shard_id: str) -> datetime | None:
    try:
        response = watermark_table.get_item(Key={"shard_id": shard_id})
    except ClientError as e:
        logger.error("watermark_read_failed shard=%s error=%s", shard_id, e,
                     extra={"shard_id": shard_id, "error": str(e)})
        # Fail the batch. Pretending there is no watermark would classify every
        # record in the batch as on time and could lower the stored watermark.
        raise
    if "Item" in response:
        return _parse_stored_watermark(response["Item"]["watermark"])
    return None


def _set_watermark(shard_id: str, watermark: datetime):
    # An UNCONDITIONAL put, on purpose (see SCENARIO D in the module docstring).
    # ReturnValues="ALL_OLD" hands back what was stored before, so we can SEE a regression.
    response = watermark_table.put_item(
        Item={
            "shard_id":  shard_id,
            "watermark": watermark.isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        },
        ReturnValues="ALL_OLD",
    )
    old = response.get("Attributes")
    if old and "watermark" in old:
        old_watermark = _parse_stored_watermark(old["watermark"])
        if old_watermark > watermark:
            regressed_by = (old_watermark - watermark).total_seconds()
            logger.warning(
                "watermark_regressed shard=%s old=%s new=%s regressed_by_seconds=%.1f",
                shard_id, old_watermark.isoformat(), watermark.isoformat(), regressed_by,
                extra={"shard_id": shard_id,
                       "old_watermark": old_watermark.isoformat(),
                       "new_watermark": watermark.isoformat(),
                       "regressed_by_seconds": regressed_by})


def _update_window_aggregate(window_start: str, metric_name: str, agg: dict):
    """
    Additive update - ADD page_views/clicks/purchases, and merge unique_users
    into a DynamoDB string set (SS). This means re-processing the same batch
    twice (e.g. on Lambda retry) would double-count page_views - a real
    at-least-once consideration. In production you would use the Kinesis
    sequence number as an idempotency key. This is flagged as a discussion
    point for the Defend phase, not solved in this lab (keeps the core
    windowing logic legible).
    """
    update_expr = (
        "SET page_views = if_not_exists(page_views, :zero) + :pv, "
        "    clicks     = if_not_exists(clicks, :zero) + :c, "
        "    purchases  = if_not_exists(purchases, :zero) + :p, "
        "    updated_at = :ts "
        "ADD unique_users :users"
    )
    window_table.update_item(
        Key={"window_start": window_start, "metric_name": metric_name},
        UpdateExpression=update_expr,
        ExpressionAttributeValues={
            ":zero":  0,
            ":pv":    agg["page_views"],
            ":c":     agg["clicks"],
            ":p":     agg["purchases"],
            ":users": set(agg["unique_users"]) if agg["unique_users"] else {"__none__"},
            ":ts":    datetime.now(timezone.utc).isoformat(),
        },
    )


def _extract_shard_id(event_id: str) -> str:
    """
    Kinesis eventID format: "shardId-000000000001:49590...sequence..."
    We use the shard portion as our watermark partition key.
    """
    return event_id.split(":")[0]
