"""
LateDataHandler Lambda
=======================
Triggered by: SQS late-events queue (batch_size=10)

Does:
  Counts late events into a SEPARATE "late aggregates" DynamoDB table,
  keyed by the SAME (window_start, metric_name) as the original window -
  but in a different table.

WHY A SEPARATE TABLE INSTEAD OF UPDATING window_aggregates DIRECTLY:
  1. Stability: window_aggregates represents "what we reported as final
     for this window." If late data silently mutated it, any downstream
     consumer (a dashboard, a billing calculation, an SLA report) that
     already read the "final" value would now be silently wrong with no
     trace of the correction.
  2. Visibility: late_aggregates existing at all is itself a signal -
     it tells you HOW MUCH late data your pipeline receives, and
     max_lateness_seconds tells you how far past the cutoff the worst
     straggler arrived - which is the number to look at when tuning
     GRACE_PERIOD_SECONDS.
  3. Explicit reconciliation: a separate downstream process (or human)
     can decide whether and how to merge late_aggregates into reporting -
     e.g. "republish corrected hourly rollups" - as a deliberate action,
     not an automatic silent mutation.

This is the architectural answer to "what do you do with late data?" -
the answer is NOT "ignore it" and NOT "silently fix the old number."
It's "record it transparently and let a human or downstream process decide."

WHAT THIS TABLE IS, AND IS NOT:
  It stores COUNTS per window (page views, clicks, purchases, unique users,
  how many late events, and the largest lateness). It is not a copy of the
  individual late events: those live in the SQS message until this function
  deletes it, and in the dead-letter queue if processing keeps failing. If you
  need a true per-event audit trail, store the event ids as well.

AT-LEAST-ONCE: an SQS standard queue can deliver a message more than once, and
  a failed batch is retried, so a late event can occasionally be counted twice
  (the same limitation as the aggregator; a discussion point for Defend).
"""
import json
import logging
import os
from datetime import datetime, timezone
from decimal import Decimal

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

dynamodb = boto3.resource("dynamodb")
LATE_TABLE = os.environ["LATE_TABLE"]
late_table = dynamodb.Table(LATE_TABLE)


def lambda_handler(event, context):
    batch_item_failures = []

    for record in event["Records"]:
        message_id = record["messageId"]
        try:
            _process_late_record(record)
        except Exception as exc:
            logger.error("late_record_processing_failed message_id=%s error=%s", message_id, exc,
                         extra={"message_id": message_id, "error": str(exc)})
            batch_item_failures.append({"itemIdentifier": message_id})

    return {"batchItemFailures": batch_item_failures}


def _process_late_record(record: dict):
    late = json.loads(record["body"])
    event        = late["event"]
    window_start = late["window_start"]
    lateness_s   = late["lateness_seconds"]

    metric_name = "metrics"

    # Additive update into the LATE table - never touches window_aggregates
    event_type = event["event_type"]
    update_expr = (
        "SET page_views = if_not_exists(page_views, :zero) + :pv, "
        "    clicks     = if_not_exists(clicks, :zero) + :c, "
        "    purchases  = if_not_exists(purchases, :zero) + :p, "
        "    late_event_count = if_not_exists(late_event_count, :zero) + :one, "
        "    last_late_at = :ts "
        "ADD unique_users :users"
    )

    late_table.update_item(
        Key={"window_start": window_start, "metric_name": metric_name},
        UpdateExpression=update_expr,
        ExpressionAttributeValues={
            ":zero":  0,
            ":one":   1,
            ":pv":    1 if event_type == "page_view" else 0,
            ":c":     1 if event_type == "click" else 0,
            ":p":     1 if event_type == "purchase" else 0,
            ":users": {str(event["user_id"])},
            ":ts":    datetime.now(timezone.utc).isoformat(),
        },
    )

    # Track the maximum lateness observed - useful for tuning GRACE_PERIOD_SECONDS
    _record_max_lateness(window_start, metric_name, lateness_s)

    logger.info("late_event_recorded window_start=%s event_id=%s lateness_seconds=%s",
                window_start, event["event_id"], lateness_s,
                extra={"window_start": window_start,
                       "event_id": event["event_id"],
                       "lateness_seconds": lateness_s})


def _record_max_lateness(window_start: str, metric_name: str, lateness_s):
    """
    Keep the LARGEST lateness seen for this window.

    DynamoDB has no MAX() inside an UpdateExpression, but a conditional write does
    the job without a read-then-write race: set the new value only if there is no
    stored value yet or the stored one is smaller. If the stored maximum is already
    at least as large, DynamoDB rejects the write with ConditionalCheckFailedException,
    which here simply means "nothing to do".

    DynamoDB numbers must be Decimal: the boto3 resource rejects a Python float.
    """
    lateness = Decimal(str(lateness_s))
    try:
        late_table.update_item(
            Key={"window_start": window_start, "metric_name": metric_name},
            UpdateExpression="SET max_lateness_seconds = :lateness",
            ConditionExpression=("attribute_not_exists(max_lateness_seconds) OR "
                                 "max_lateness_seconds < :lateness"),
            ExpressionAttributeValues={":lateness": lateness},
        )
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "ConditionalCheckFailedException":
            raise
