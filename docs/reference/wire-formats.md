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
| dynamodb | `(feed_url, item_id)` PK | item attributes per `Change` field | Idempotent `PutItem` (upsert); optional Number TTL attribute. Consume via Streams/polling. |
| gcp_pubsub | `OrderingKey` (optional) | JSON `Change` `Data` | Attributes: same as Kafka headers.                              |
| azureservicebus | `MessageID = item_id` | JSON `Change` body | ApplicationProperties: same keys as Kafka headers; `ContentType: application/json`. |
| cosmosdb | `id = sha256(feed_url \0 item_id)`, partition `/feed_url` | JSON `Change` document (with added `id`) | Stored as a document; idempotent on `id` (409 Conflict swallowed). |
| dapr_pubsub | n/a (broker-defined) | JSON `Change` data (CloudEvent by default) | Metadata: same keys as Kafka headers, plus any static `metadata`. |
| nats     | subject (static)         | JSON `Change` `Data` | NATS headers: same keys as Kafka headers. |
| grpc     | n/a                      | Typed `Change` protobuf (`ChangeSink.Publish`) | Metadata: `rss2msg-schema-version`, `rss2msg-feed-url`, `rss2msg-item-id`, `rss2msg-kind`, `rss2msg-dlq-*?`; W3C trace context. |
| feed     | entry id `urn:rss2msg:<sha256>` | RSS 2.0 / Atom 1.0 document (windowed) | Served over HTTP; per-entry mapping below. |

Postgres `payload` is the full envelope — everything else is extractable
from it; the columns are for indexing and basic SQL filtering.

The [amqp091](../how-to/sinks/amqp091.md),
[azureservicebus](../how-to/sinks/azureservicebus.md),
[cosmosdb](../how-to/sinks/cosmosdb.md),
[nats](../how-to/sinks/nats.md),
[stdout](../how-to/sinks/stdout.md), [http](../how-to/sinks/http.md), and
[grpc](../how-to/sinks/grpc.md) sinks document their publish layout on their own
pages.

## Feed sink (RSS 2.0 / Atom 1.0)

The [feed](../how-to/sinks/feed.md) sink does not emit one message per change.
It serves a rolling window of recent changes as a single document over HTTP:
RSS 2.0 at `rss.path` and Atom 1.0 at `atom.path`.

Each feed entry id is a synthetic, globally-unique URN derived from the change's
`(feed_url, item_id)`: `urn:rss2msg:<sha256(feed_url + "\n" + item_id)>`. RSS
renders it as `<guid isPermaLink="false">`. The synthetic id is required because
`item_id` is only unique within a single source feed, whereas an aggregated feed
must give every entry a globally-unique id.

`Change` → feed entry mapping:

| feed entry           | source |
| -------------------- | ------ |
| title                | `Title`, falling back to `(untitled)` when empty. |
| link (`rel=alternate`) | `Link`, when set. |
| description          | `Summary`; when empty, `Content` truncated to 512 bytes (on a UTF-8 rune boundary). |
| content              | `Content`. |
| author               | `Authors[0]`, when present. |
| created / published  | `PublishedAt`, falling back to `DetectedAt`. |
| updated              | `UpdatedAt`, falling back to created. |

The Atom feed includes a `rel=self` link built from `public_url` (or `link`)
plus the endpoint path. Atom `feed:updated` is the newest `DetectedAt` in the
window (or the current time when the window is empty).

## Related

- [Change Envelope](change-envelope.md) — the payload these formats carry.
- [Choose a Sink](../how-to/choose-a-sink.md) — picking and configuring a sink.
