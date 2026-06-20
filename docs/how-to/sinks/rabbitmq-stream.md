---
title: rabbitmq_stream sink
type: how-to
tags: [rss2msg/docs, sinks, rabbitmq_stream, rabbitmq, streams]
summary: Publish Changes to a RabbitMQ Stream over the native stream protocol (port 5552) with synchronous publish confirmation.
updated: 2026-06-20
---

# rabbitmq_stream sink

Publishes each `Change` to a [RabbitMQ Stream](https://www.rabbitmq.com/docs/streams)
over the **native stream protocol** (default port **5552**), not AMQP 0-9-1. Use the
[amqp091 sink](./amqp091.md) for classic exchanges/queues; use this sink when you want
RabbitMQ's append-only, replayable stream log.

```yaml
- name: stream-main
  driver: rabbitmq_stream
  rabbitmq_stream:
    uris: ["rabbitmq-stream://guest:guest@rabbit-1:5552/%2f"]  # or a single url:
    stream: feed.changes                          # target stream (required)
    declare: true                                 # create the stream if absent
    max_age: 168h                                 # optional retention (declare only)
    max_length_bytes: 5368709120                  # optional retention (declare only)
    # username: ${RMQ_USER}                       # optional; else URI userinfo
    # password: ${RMQ_PASS}
    tls:                                          # custom CA / mTLS
      ca_file: /etc/ssl/certs/rabbit-ca.pem
  dead_letter: dlq-main
```

| field              | required | default | notes |
| ------------------ | -------- | ------- | ----- |
| `uris`             | yes¹     | —       | One or more `rabbitmq-stream://[user:pass@]host:5552/vhost` URIs. The client load-balances across them. |
| `url`              | yes¹     | —       | Single-URI shorthand; used only when `uris` is empty. |
| `stream`           | yes      | —       | Target stream name. |
| `username`         | no       | —       | Overrides any userinfo in the URI(s). |
| `password`         | no       | —       | Overrides any userinfo in the URI(s). |
| `declare`          | no       | `false` | Create the stream at startup if it does not exist (idempotent; an already-existing stream is not an error). |
| `max_age`          | no       | `0`     | Retention by age, applied only when declaring. `0` leaves it unset. |
| `max_length_bytes` | no       | `0`     | Retention by total size in bytes, applied only when declaring. `0` leaves it unset. |
| `tls`              | no       | (off)   | Structured TLS (custom CA / mTLS / verification). See [Secure Connections (TLS)](../secure-connections-tls.md#sinks). |

¹ Exactly one of `uris` or `url` is required.

**Auth precedence:** explicit `username` / `password` win over any userinfo embedded in
the URI(s).

**TLS:** when the `tls` block is active the environment connects with TLS enabled. Setting
`insecure_skip_verify: true` disables verification and is logged at warn. Custom CA and
client certificates (mTLS) work as for the other sinks.

Publish layout:
- Body: JSON `Change` envelope.
- AMQP 1.0 message properties: `ContentType = application/json`,
  `MessageID = Change.ItemID`.
- Application properties: `feed_url`, `kind`, `schema_version`, optional `traceparent` /
  `tracestate`, optional `dlq_from_sink` / `dlq_error` / `dlq_attempts`.

Implementation notes:
- One stream `Environment` + one `Producer` per Publisher.
- Publishes are **synchronously confirmed**: `Publish` serialises one in-flight message
  under a mutex and blocks on the broker's confirmation channel, so a returned `nil`
  means the broker confirmed the message. An unconfirmed status surfaces as a publish
  error and is handled by the sink retry+DLQ layer.
- **Publisher-side deduplication is not yet supported.** The stream client can dedupe via
  a `producer_name` plus a monotonic numeric `publishingID`, but `Change.ItemID` is a
  non-monotonic string; v1 publishes without dedup.

## Related

- [Choose a Sink](../choose-a-sink.md) — all drivers and the decision table.
- [amqp091 sink](./amqp091.md) — classic AMQP 0-9-1 exchanges/queues.
- [Secure Connections (TLS)](../secure-connections-tls.md#sinks) — custom CA / mTLS for this sink.
- [Sink Wire Formats](../../reference/wire-formats.md) — on-the-wire layout.
- [Change Envelope](../../reference/change-envelope.md) — the payload body.
