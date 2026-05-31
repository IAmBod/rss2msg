---
title: SQS sink
type: how-to
tags: [rss2msg/docs, sinks, sqs, aws]
summary: Send Changes to an SQS queue; standard vs FIFO, message groups, and dedup ids.
updated: 2026-05-30
---

# SQS sink

```yaml
- name: sqs-main
  driver: sqs
  sqs:
    queue_url: https://sqs.us-east-1.amazonaws.com/123456789012/feed-changes
    region: us-east-1
    # endpoint_url: http://localhost:4566   # LocalStack
    # message_group: feed_url               # FIFO only — see below
```

| field           | required | notes |
| --------------- | -------- | ----- |
| `queue_url`     | yes      | Full SQS URL. A `.fifo` suffix selects FIFO mode (see below). |
| `region`        | no       | AWS SDK falls back to env/profile. |
| `endpoint_url`  | no       | Override for LocalStack-style endpoints. |
| `message_group` | no       | FIFO only: `feed_url` (default) \| `item_id` \| `sink`. Rejected when set on a standard (non-FIFO) queue. |

Credentials come from the standard AWS SDK credential chain
(`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`, shared `~/.aws/credentials`,
instance metadata, etc.).

Message body = JSON `Change` envelope. Message attributes: `feed_url`,
`kind`, `schema_version`, optional `traceparent` / `tracestate`, optional
DLQ annotations.

## FIFO queues

When `queue_url` ends with `.fifo`, the sink sets the two FIFO-required
fields on every `SendMessage` call:

- **`MessageGroupId`** — derived from `message_group`:
  - `feed_url` (default) — one group per feed: in-order per feed, parallel across feeds.
  - `item_id` — one group per item: maximum parallelism; only useful when the consumer doesn't need cross-item ordering.
  - `sink` — single group across the entire sink: strict global ordering, no parallelism.
- **`MessageDeduplicationId`** — `sha256(feed_url || item_id || content_hash)` rendered as hex. Re-publishes of an unchanged Change within SQS's 5-minute dedup window are coalesced; updates (content hash changes) produce a fresh dedup id and are delivered.

The dedup id we send is honoured regardless of the queue's
ContentBasedDeduplication setting.

## Related

- [Choose a Sink](../choose-a-sink.md) — all drivers and the decision table.
- [Sink Wire Formats](../../reference/wire-formats.md) — on-the-wire layout.
- [Change Envelope](../../reference/change-envelope.md) — the payload body.
