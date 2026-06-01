---
title: Kafka sink
type: how-to
tags: [rss2msg/docs, sinks, kafka]
summary: Publish Changes to a topic with configurable acks and compression; record/header layout.
updated: 2026-05-30
---

# Kafka sink

```yaml
- name: kafka-main
  driver: kafka
  kafka:
    brokers: ["kafka-1:9092", "kafka-2:9092"]
    topic: feed.changes
    acks: all
    compression: snappy
```

| field         | required | default       | values |
| ------------- | -------- | ------------- | ------ |
| `brokers`     | yes      | —             | List of `host:port`. |
| `topic`       | yes      | —             | Topic name; client does not auto-create. |
| `acks`        | no       | `all`         | `all` \| `leader` \| `none`. **`none` is unsafe** (see [Operational Notes](../../explanation/operations.md)). |
| `compression` | no       | `none`        | `none` \| `snappy` \| `lz4` \| `zstd` \| `gzip`. |
| `tls`         | no       | (off)         | Structured TLS to the brokers. Kafka has no URL scheme, so set `tls.enabled: true` to turn it on. See [Secure Connections (TLS)](../secure-connections-tls.md#sinks). |

> [!warning]
> `acks: none` is unsafe. Combined with the commit-on-success model it can drop messages without the state store knowing. See [Operational Notes](../../explanation/operations.md). Use the default `all` unless you accept the trade-off.

Record layout:
- `Key` = `Change.ItemID` (so consumers can co-partition by item).
- `Value` = JSON `Change` envelope.
- Headers: `feed_url`, `kind`, `schema_version`, optional `traceparent` /
  `tracestate`, optional `dlq_from_sink` / `dlq_error` / `dlq_attempts`.

## Related

- [Choose a Sink](../choose-a-sink.md) — all drivers and the decision table.
- [Secure Connections (TLS)](../secure-connections-tls.md#sinks) — custom CA / mTLS for this sink.
- [Sink Wire Formats](../../reference/wire-formats.md) — on-the-wire layout.
- [Change Envelope](../../reference/change-envelope.md) — the payload body.
