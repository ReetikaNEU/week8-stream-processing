#!/usr/bin/env python3
"""
Event load generator for Week 8.

Usage:
  export API_ENDPOINT=https://xxx.execute-api.us-east-1.amazonaws.com/prod/

  # Normal traffic: event_time ~= now (on-time events)
  python3 scripts/generate_events.py normal --count 200 --rate 20

  # Late traffic: event_time is N seconds in the past
  python3 scripts/generate_events.py late --count 50 --lateness 90

  # Mixed: simulates mobile clients that buffer offline then burst-send
  python3 scripts/generate_events.py mixed --count 200 --rate 20
"""
import argparse
import json
import os
import sys
import time
import uuid
import random
import urllib.request
import urllib.error
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

API_ENDPOINT = os.environ.get("API_ENDPOINT", "")
EVENT_TYPES  = ["page_view", "click", "purchase", "scroll"]
PAGES        = ["/home", "/products", "/cart", "/checkout", "/profile"]


def send_event(user_id: str, event_type: str, event_time: datetime, page: str):
    payload = json.dumps({
        "user_id":    user_id,
        "event_type": event_type,
        "page":       page,
        "event_time": event_time.isoformat(),
    }).encode()

    req = urllib.request.Request(
        f"{API_ENDPOINT}events",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status
    except urllib.error.HTTPError as e:
        return e.code
    except Exception as e:
        return str(e)


def normal(count: int, rate: int):
    """event_time = now (or very close to it). Establishes baseline."""
    print(f"Sending {count} on-time events at {rate}/s...")
    interval = 1.0 / rate
    success = 0
    with ThreadPoolExecutor(max_workers=10) as pool:
        futures = []
        for i in range(count):
            now = datetime.now(timezone.utc)
            user_id = f"user-{i % 50:03d}"   # 50 distinct users
            event_type = random.choice(EVENT_TYPES)
            page = random.choice(PAGES)
            futures.append(pool.submit(send_event, user_id, event_type, now, page))
            time.sleep(interval)

        for f in as_completed(futures):
            if f.result() == 202:
                success += 1

    print(f"Done. {success}/{count} accepted (202).")


def late(count: int, lateness: int):
    """
    event_time = now - lateness seconds.
    With WINDOW_SIZE_SECONDS=60 and GRACE_PERIOD_SECONDS=30, a window closes
    90 seconds after it starts. lateness=90 means these events arrive for a
    window that closed the moment the watermark advances past their
    window's close threshold.

    lateness < 90:  events land in an OPEN window (processed normally)
    lateness > 90:  events land in a CLOSED window (routed to late-events queue)
    """
    print(f"Sending {count} events with event_time = now - {lateness}s...")
    past_time = datetime.now(timezone.utc) - timedelta(seconds=lateness)

    success = 0
    with ThreadPoolExecutor(max_workers=10) as pool:
        futures = []
        for i in range(count):
            user_id = f"late-user-{i % 10:03d}"
            event_type = random.choice(EVENT_TYPES)
            page = random.choice(PAGES)
            futures.append(pool.submit(send_event, user_id, event_type, past_time, page))

        for f in as_completed(futures):
            if f.result() == 202:
                success += 1

    print(f"Done. {success}/{count} accepted (202).")
    print(f"\nevent_time for all events: {past_time.isoformat()}")
    print(f"window_start for these events: "
          f"{(int(past_time.timestamp()) // 60 * 60)}")

    if lateness > 90:
        print("\n>>> lateness > 90s (window_size + grace_period)")
        print(">>> EXPECT: these events route to the late-events SQS queue")
        print(">>> Check: python3 scripts/break_scenarios.py check_late_queue")
    else:
        print("\n>>> lateness <= 90s - window may still be open")
        print(">>> EXPECT: events update window_aggregates normally")


def mixed(count: int, rate: int):
    """
    Simulates a realistic mobile scenario:
      80% of events: event_time ~= now (online users)
      20% of events: event_time = now - random(60, 180)s
                      (users who were offline and just reconnected,
                       sending a backlog of buffered events)
    """
    print(f"Sending {count} mixed-timing events at {rate}/s "
          f"(80% on-time, 20% delayed 60-180s)...")
    interval = 1.0 / rate
    on_time = late_count = 0

    with ThreadPoolExecutor(max_workers=10) as pool:
        futures = []
        for i in range(count):
            now = datetime.now(timezone.utc)
            user_id = f"user-{i % 50:03d}"
            event_type = random.choice(EVENT_TYPES)
            page = random.choice(PAGES)

            if random.random() < 0.2:
                event_time = now - timedelta(seconds=random.randint(60, 180))
                late_count += 1
            else:
                event_time = now
                on_time += 1

            futures.append(pool.submit(send_event, user_id, event_type, event_time, page))
            time.sleep(interval)

        success = sum(1 for f in as_completed(futures) if f.result() == 202)

    print(f"Done. {success}/{count} accepted.")
    print(f"  On-time events sent: {on_time}")
    print(f"  Delayed events sent: {late_count} (event_time 60-180s in the past)")


if __name__ == "__main__":
    if not API_ENDPOINT:
        print("ERROR: set API_ENDPOINT env var")
        sys.exit(1)

    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["normal", "late", "mixed"])
    parser.add_argument("--count", type=int, default=100)
    parser.add_argument("--rate", type=int, default=10, help="events/sec (normal, mixed)")
    parser.add_argument("--lateness", type=int, default=90, help="seconds in the past (late)")
    args = parser.parse_args()

    if args.mode == "normal":
        normal(args.count, args.rate)
    elif args.mode == "late":
        late(args.count, args.lateness)
    elif args.mode == "mixed":
        mixed(args.count, args.rate)
