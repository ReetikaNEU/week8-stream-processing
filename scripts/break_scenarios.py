#!/usr/bin/env python3
"""
Break phase helper scripts for Week 8.

Usage:
  export WINDOW_TABLE=week8-window-aggregates
  export LATE_TABLE=week8-late-aggregates
  export WATERMARK_TABLE=week8-watermarks
  export LATE_QUEUE_URL=<from cdk output>
  export CLICKSTREAM_NAME=week8-clickstream

  python3 scripts/break_scenarios.py show_windows
  python3 scripts/break_scenarios.py show_watermarks
  python3 scripts/break_scenarios.py check_late_queue
  python3 scripts/break_scenarios.py show_late_aggregates
  python3 scripts/break_scenarios.py reset_watermarks
"""
import boto3
import json
import os
import sys
import time
from datetime import datetime, timezone
from boto3.dynamodb.conditions import Key

dynamodb   = boto3.resource("dynamodb")
sqs_client = boto3.client("sqs")
kinesis_client = boto3.client("kinesis")

WINDOW_TABLE     = os.environ.get("WINDOW_TABLE", "week8-window-aggregates")
LATE_TABLE       = os.environ.get("LATE_TABLE", "week8-late-aggregates")
WATERMARK_TABLE  = os.environ.get("WATERMARK_TABLE", "week8-watermarks")
LATE_QUEUE_URL   = os.environ.get("LATE_QUEUE_URL", "")
CLICKSTREAM_NAME = os.environ.get("CLICKSTREAM_NAME", "week8-clickstream")


def show_windows():
    """Print all window aggregates, sorted by window_start."""
    table = dynamodb.Table(WINDOW_TABLE)
    response = table.scan()
    items = sorted(response["Items"], key=lambda x: x["window_start"])

    if not items:
        print("No window aggregates yet. Send some events first:")
        print("  python3 scripts/generate_events.py normal --count 100 --rate 10")
        return

    print(f"{'Window Start':<28} {'Page Views':>10} {'Clicks':>8} "
          f"{'Purchases':>10} {'Unique Users':>13}")
    print("-" * 75)
    for item in items:
        unique = item.get("unique_users", set())
        unique_count = len([u for u in unique if u != "__none__"]) if unique else 0
        print(f"{item['window_start']:<28} "
              f"{int(item.get('page_views', 0)):>10} "
              f"{int(item.get('clicks', 0)):>8} "
              f"{int(item.get('purchases', 0)):>10} "
              f"{unique_count:>13}")


def show_watermarks():
    """Print current watermark per shard, and how far behind 'now' each is."""
    table = dynamodb.Table(WATERMARK_TABLE)
    response = table.scan()
    items = response["Items"]

    if not items:
        print("No watermarks yet. Send some events first.")
        return

    now = datetime.now(timezone.utc)
    print(f"{'Shard':<25} {'Watermark':<32} {'Behind now by':<15}")
    print("-" * 75)
    for item in items:
        wm = datetime.fromisoformat(item["watermark"])
        behind = (now - wm).total_seconds()
        print(f"{item['shard_id']:<25} {item['watermark']:<32} {behind:>6.1f}s")


def check_late_queue():
    """Show late-events queue depth and DLQ depth."""
    if not LATE_QUEUE_URL:
        print("Set LATE_QUEUE_URL env var")
        return

    attrs = sqs_client.get_queue_attributes(
        QueueUrl=LATE_QUEUE_URL,
        AttributeNames=["ApproximateNumberOfMessages",
                        "ApproximateNumberOfMessagesNotVisible"]
    )["Attributes"]

    print(f"Late events queue:")
    print(f"  Visible:   {attrs['ApproximateNumberOfMessages']}")
    print(f"  In-flight: {attrs['ApproximateNumberOfMessagesNotVisible']}")


def show_late_aggregates():
    """Print all late/corrected aggregates."""
    table = dynamodb.Table(LATE_TABLE)
    response = table.scan()
    items = sorted(response["Items"], key=lambda x: x["window_start"])

    if not items:
        print("No late aggregates. Either no late events occurred, "
              "or LateDataHandler hasn't processed them yet (wait ~10s).")
        return

    #~dk
    #print(f"{'Window Start (closed)':<28} {'Late Page Views':>15} "
    #      f"{'Late Clicks':>12} {'Late Events Total':>18}")
    #print("-" * 78)
    #for item in items:
    #    print(f"{item['window_start']:<28} "
    #          f"{int(item.get('page_views', 0)):>15} "
    #          f"{int(item.get('clicks', 0)):>12} "
    #          f"{int(item.get('late_event_count', 0)):>18}")
    print(f"{'Window Start (closed)':<28} {'Late Page Views':>15} "
          f"{'Late Clicks':>12} {'Late Events Total':>18} {'Max Lateness (s)':>17}")
    print("-" * 96)
    for item in items:
        print(f"{item['window_start']:<28} "
              f"{int(item.get('page_views', 0)):>15} "
              f"{int(item.get('clicks', 0)):>12} "
              f"{int(item.get('late_event_count', 0)):>18} "
              f"{float(item.get('max_lateness_seconds', 0)):>17.1f}")

    print(f"\n>>> These windows were ALREADY CLOSED when these events arrived.")
    print(f">>> window_aggregates table was NOT modified.")
    print(f">>> Compare: python3 scripts/break_scenarios.py show_windows")


def reset_watermarks():
    """
    Delete all watermark records. Use this to reset the pipeline to a
    'fresh start' state - e.g. before re-running a Break scenario from
    scratch. Does NOT delete window_aggregates or late_aggregates.
    """
    table = dynamodb.Table(WATERMARK_TABLE)
    response = table.scan()
    items = response["Items"]

    if not items:
        print("No watermarks to reset.")
        return

    with table.batch_writer() as batch:
        for item in items:
            batch.delete_item(Key={"shard_id": item["shard_id"]})

    print(f"Deleted {len(items)} watermark records.")
    print("Next batch of events will establish fresh watermarks from scratch.")


def describe_shards():
    """Show the Kinesis stream's shard IDs - useful for matching watermark records."""
    response = kinesis_client.describe_stream(StreamName=CLICKSTREAM_NAME)
    shards = response["StreamDescription"]["Shards"]
    print(f"Stream {CLICKSTREAM_NAME} has {len(shards)} shards:")
    for shard in shards:
        print(f"  {shard['ShardId']}")


COMMANDS = {
    "show_windows":         show_windows,
    "show_watermarks":      show_watermarks,
    "check_late_queue":     check_late_queue,
    "show_late_aggregates": show_late_aggregates,
    "reset_watermarks":     reset_watermarks,
    "describe_shards":      describe_shards,
}

if __name__ == "__main__":
    if len(sys.argv) < 2 or sys.argv[1] not in COMMANDS:
        print("Usage: python3 break_scenarios.py <command>")
        print(f"Commands: {', '.join(COMMANDS)}")
        sys.exit(1)
    COMMANDS[sys.argv[1]]()
