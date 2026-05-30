---
title: Sink Wire Formats
type: reference
tags: [rss2msg/docs, sinks, output]
summary: Per-sink key, body, and metadata layout for the published Change envelope.
updated: 2026-05-30
---

# Sink Wire Formats

| sink     | key / partition          | body                | metadata                                                          |
| -------- | ------------------------ | ------------------- | ----------------------------------------------------------------- |
| postgres | `(feed_url, item_id, detected_at)` PK | JSONB `payload`     | Columns: `feed_url`, `item_id`, `kind`, `detected_at`.            |
| kafka    | `Key = item_id`          | JSON `Change` value | Headers: `feed_url`, `kind`, `schema_version`, `traceparent?`, `tracestate?`, `dlq_*?`. |
| sqs      | n/a                      | JSON `Change` body  | MessageAttributes: same as Kafka headers.                         |
| sns      | n/a                      | JSON `Change` body  | MessageAttributes: same as Kafka headers.                         |

Postgres `payload` is the full envelope — everything else is extractable
from it; the columns are for indexing and basic SQL filtering.

## Related

- [Change Envelope](change-envelope.md) — the payload these formats carry.
- [Choose a Sink](../how-to/choose-a-sink.md) — picking and configuring a sink.
