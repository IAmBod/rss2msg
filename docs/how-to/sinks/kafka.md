---
title: Kafka sink
type: how-to
tags: [rss2msg/docs, sinks, kafka]
summary: Publish Changes to a topic with configurable acks and compression; record/header layout.
updated: 2026-06-15
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
| `schema_registry` | no | (off) | Confluent Schema Registry encoding of the record value. Absent ⇒ plain JSON. See [below](#schema-registry-optional). |

> [!warning]
> `acks: none` is unsafe. Combined with the commit-on-success model it can drop messages without the state store knowing. See [Operational Notes](../../explanation/operations.md). Use the default `all` unless you accept the trade-off.

Record layout:
- `Key` = `Change.ItemID` (so consumers can co-partition by item).
- `Value` = JSON `Change` envelope (plain), or a Confluent-framed value (magic byte + schema ID + JSON payload) when `schema_registry` is configured.
- Headers: `feed_url`, `kind`, `schema_version`, optional `traceparent` /
  `tracestate`, optional `dlq_from_sink` / `dlq_error` / `dlq_attempts`.

## Schema Registry (optional)

Set `schema_registry.url` to frame the record value with the Confluent wire
format (magic byte + 4-byte schema ID + payload) and register a schema with a
Confluent-compatible Schema Registry. Absent, the value is plain JSON exactly as
before — this is fully opt-in and configured per kafka sink.

```yaml
- name: events
  driver: kafka
  kafka:
    brokers: ["kafka:9092"]
    topic: feed.changes
    schema_registry:
      url: http://schema-registry:8081  # presence enables the feature
      format: json                      # json, avro, or protobuf
      subject: feed.changes-value       # default <topic>-value
      auto_register: true               # default true
      schema_file: ./change.schema.json # optional: overrides the registered schema text
      basic_auth:
        username: sruser
        password: ${SR_PASSWORD}
```

| field | required | default | values |
| --- | --- | --- | --- |
| `url` | yes (to enable) | — | Schema Registry base URL. Its presence turns the feature on. |
| `format` | yes (when url set) | — | `json`, `avro`, or `protobuf`. |
| `subject` | no | `<topic>-value` | Subject name (TopicNameStrategy). |
| `auto_register` | no | `true` | Register the schema on first publish; `false` looks up an existing id and errors if absent. |
| `schema_file` | no | (canonical) | Overrides the registered schema text; must stay wire-compatible with the canonical `Change` shape. |
| `basic_auth` | no | (none) | `username` / `password` for the registry. |
| `tls` | no | (off) | TLS to the registry; same shape as the broker `tls` block. `insecure_skip_verify: true` is logged at warn. |

The canonical JSON Schema is generated from the `Change` envelope. When enabled,
registration or encoding errors **hard-fail** the publish, so unframed records
never land.

When `format: avro`, the Avro encoder uses a canonical schema generated from the
`Change` envelope, with `timestamp-micros` logical types for timestamps and nullable
unions for optional times; like JSON, the registered schema text can be overridden
with `schema_file`.

When `format: protobuf`, the Protobuf encoder reuses the canonical `Change` message
defined in `proto/sink/v1`, which uses `google.protobuf.Timestamp` fields for
timestamps. The Confluent wire format uses a 5-byte header (magic byte + schema ID)
followed by a 1-byte message-index (always `0x00` for the first message in the
schema file), then the raw proto3-serialised bytes. Like the other formats, the
registered schema text can be overridden with `schema_file`.

## Related

- [Choose a Sink](../choose-a-sink.md) — all drivers and the decision table.
- [Secure Connections (TLS)](../secure-connections-tls.md#sinks) — custom CA / mTLS for this sink.
- [Sink Wire Formats](../../reference/wire-formats.md) — on-the-wire layout.
- [Change Envelope](../../reference/change-envelope.md) — the payload body.
- [Send to Discord and Slack with Redpanda Connect](../integrations/redpanda-connect.md) — consume this topic and fan out to chat.
