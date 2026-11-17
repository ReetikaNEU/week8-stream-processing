# INFO7375 Week 8 — Email Service Design

**Reetika Bhanushali | Branch: `email-server`**

**Scope.** Extend the existing Week 8 stream-processing repository into an API-based email ingestion, delivery, and mailbox-reading prototype. This is not an SMTP/IMAP server.

## Architecture

`POST /emails` → API Gateway → existing EventProducer Lambda → Kinesis (`week8-clickstream`, partitioned by recipient) → existing WindowAggregator Lambda → DynamoDB (`week8-email-mailboxes`).

- **Invalid email:** Producer → DynamoDB quarantine table with rejection reasons.
- **Late email:** Aggregator → existing SQS late-events queue → LateDataHandler → mailbox `LATE` folder.
- **Reading:** New EmailReader Lambda provides mailbox listing, individual message viewing, folder filtering, keyword search, and date-range queries.
- **Backward compatibility:** Existing `POST /events` and clickstream processing remain functional.

## Key Design Decisions

1. **Kinesis and recipient partitioning:** Reuse the original stream and Lambda consumer. Partitioning by normalized recipient keeps a mailbox's events on the same shard. A busy recipient may create uneven shard traffic.

2. **Event time and watermarks:** Use the email's validated Date field as event time and preserve ingestion time separately. Reuse 60-second windows with 30-second grace. Late events are retained, not silently discarded. Per-shard watermarks can classify a quiet recipient's messages as late when other recipients advance the shard watermark.

3. **DynamoDB and idempotency:** Store emails using `(recipient, message_id)` as the primary key. Conditional writes prevent replay duplicates. The `RecipientByDate` index uses `(recipient, event_time_id)` for newest-first and date-range queries.

4. **Search and folders:** Apply case-insensitive keyword filters and subject-based rules (INVOICE, MEETING, NEWSLETTER, DINO) when reading indexed mailbox records. Persist delivery folders as `INBOX` or `LATE`.

5. **Validation and reliability:** Reject invalid addresses, missing fields, timezone-free dates, and unacceptable future dates. Record invalid submissions in quarantine. Reuse SQS for late-event processing and retries.

## Evidence and Fit

All 52 unit tests pass, including original clickstream tests. Replaying 500 unique emails twice left the mailbox count unchanged at 503 (3 earlier records plus 500 unique emails). Busy and quiet mailbox counts were 109 and 0. Search, date filtering, viewer, and event-time ordering passed.

During the larger burst, 3,000/3,000 emails were accepted and stored. The configured rate was 100/sec; observed overall throughput was approximately 72.9/sec. Delivery p99 was 12.328 seconds versus 10.336 seconds in the smaller replay. Peak observed IteratorAge was 12.296 seconds versus 10.929 seconds.

## Limitations

The prototype has no SMTP/IMAP integration, browser UI, or user authentication. DynamoDB supports efficient recipient/date queries, but keyword filtering is not a scalable full-text search index. GSI reads are eventually consistent. Production deployment would require authentication, authorization, and potentially OpenSearch for large-scale search.
