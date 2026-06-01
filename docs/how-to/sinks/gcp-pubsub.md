---
title: Google Cloud Pub/Sub sink
type: how-to
tags: [rss2msg/docs, sinks, gcp_pubsub, gcp]
summary: Publish Changes to a Google Cloud Pub/Sub topic, with optional ordered delivery.
updated: 2026-06-01
---

# Google Cloud Pub/Sub sink

```yaml
- name: pubsub-main
  driver: gcp_pubsub
  gcp_pubsub:
    project_id: my-gcp-project
    topic_id: feed-changes
    # endpoint: localhost:8085      # Pub/Sub emulator
    # ordering_key: feed_url        # ordered delivery — see below
  dead_letter: dlq-main
```

| field          | required | notes |
| -------------- | -------- | ----- |
| `project_id`   | yes      | GCP project that owns the topic. |
| `topic_id`     | yes      | Topic short name (not the full `projects/.../topics/...` path). The topic must already exist; the sink does not create it. |
| `endpoint`     | no       | Override host for the [Pub/Sub emulator](https://cloud.google.com/pubsub/docs/emulator) (e.g. `localhost:8085`). When set, the client connects insecure and without auth. The `PUBSUB_EMULATOR_HOST` env var is also honoured natively by the client. |
| `ordering_key` | no       | Enables ordered delivery and selects the key. One of `feed_url`, `item_id`, `sink`. Empty (default) disables ordering. |

Credentials come from Application Default Credentials — `GOOGLE_APPLICATION_CREDENTIALS`,
the gcloud user credentials, or workload identity — the standard GCP SDK chain.

Message `Data` = JSON `Change` envelope. Attributes: `feed_url`, `kind`,
`schema_version`, optional `traceparent` / `tracestate`, optional DLQ
annotations — the same attribute set as the Kafka and SQS sinks.

## Ordered delivery

When `ordering_key` is set, the sink enables message ordering on the topic and
stamps each message with an `OrderingKey`, so Pub/Sub delivers messages sharing
a key in publish order:

- `feed_url` — one key per feed: in-order per feed, parallel across feeds.
- `item_id` — one key per item: maximum parallelism; only useful when the
  consumer doesn't need cross-item ordering.
- `sink` — a single key across the entire sink: strict global ordering, no
  parallelism.

The subscription must also have message ordering enabled for the consumer to
observe the order. When a publish fails on an ordered key, Pub/Sub pauses that
key; the sink calls `ResumePublish` on failure so the retry (and later messages)
are not blocked.

## Related

- [Choose a Sink](../choose-a-sink.md) — all drivers and the decision table.
- [Sink Wire Formats](../../reference/wire-formats.md) — on-the-wire layout.
- [Change Envelope](../../reference/change-envelope.md) — the payload body.
