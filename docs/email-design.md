# Week 8 — Email Server Design

## Objective
Extend the existing Week 8 Kinesis pipeline into a
server-side email delivery and mailbox system.

## Architecture
API Gateway -> Producer Lambda -> Kinesis
-> Delivery Lambda -> DynamoDB Mailboxes

Late deliveries:
Delivery Lambda -> SQS -> Late Handler -> LATE folder

Invalid messages:
Producer -> Quarantine storage

Read API:
Mailbox queries, folders, sorting, and search.

## Design Decisions

1. Partitioning
Use recipient email address as the Kinesis partition
key. This preserves ordering per mailbox without
requiring one shard per recipient.

2. Event Time
Use the email Date header as event time.
Record server ingestion time separately.
Sort inbox messages by event time.

3. Watermarks and Lateness
Reuse the Week 8 event-time watermark logic.
Start with 60-second windows and 30-second grace.
Send messages classified as late to the LATE folder.
Document limitations of per-shard watermarks.

4. Deduplication
Use recipient + Message-ID as the idempotency key.
Apply conditional DynamoDB writes so replaying
messages does not create duplicates.

5. Mailbox Storage
Store message metadata, sender, recipient,
subject, body, dates, folder, and read status
in DynamoDB.

6. Folder Rules
Route messages by subject keywords after ingestion.
Support folders for invoice, meeting, and newsletter
messages. Folders do not determine Kinesis shards.

7. Search
Provide recipient-scoped search by keyword and
date range, with inbox results sorted by email date.

8. Bad Email Handling
Quarantine malformed dates, unacceptable future
timestamps, and invalid messages.
Never silently assign incorrect dates.

9. Replay
Replaying the same dataset must not change
the final mailbox state.

10. Observability
Measure accepted, delivered, duplicate, late,
and quarantined messages, plus delivery latency.

## Known Tradeoffs
Per-shard watermarks can disagree.
Quiet mailboxes can affect event-time progress.
DynamoDB search is appropriate for our small lab
but may need a search index at larger scale.
