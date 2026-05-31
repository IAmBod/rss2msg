---
title: RabbitMQ sink
type: how-to
tags: [rss2msg/docs, sinks, rabbitmq, amqp]
summary: Publish Changes to an AMQP exchange; routing, declaration, and connection caveats.
updated: 2026-05-30
---

# RabbitMQ sink

```yaml
- name: rmq-main
  driver: rabbitmq
  rabbitmq:
    url: amqp://guest:guest@rabbit-1:5672/      # or amqps://... for TLS
    exchange: feed.changes
    exchange_type: topic          # direct (default) | topic | fanout | headers
    routing_key: feed.changes
    declare: true                 # declare the exchange at startup
    durable: true                 # only meaningful with declare=true
    mandatory: false              # broker returns unroutable messages (currently unhandled)
```

| field          | required | default  | notes |
| -------------- | -------- | -------- | ----- |
| `url`          | yes      | —        | Standard AMQP URL (`amqp://` or `amqps://`). User/password inline; `${ENV}` substitution works. |
| `exchange`     | no       | `""`     | Empty means RabbitMQ's default direct exchange (routes by `routing_key` to a queue with the same name). |
| `exchange_type`| no       | `direct` | `direct` \| `topic` \| `fanout` \| `headers`. Only used when `declare=true`. |
| `routing_key`  | no       | `""`     | Static routing key sent on every publish. |
| `declare`      | no       | `false`  | If true, declares the exchange at startup. Requires a non-empty `exchange`. |
| `durable`      | no       | `false`  | Durability flag for the declared exchange. |
| `mandatory`    | no       | `false`  | Publish with the AMQP mandatory flag. Returns from the broker for unroutable messages are not currently handled — turning this on without a guaranteed binding effectively drops them silently. |

Publish layout:
- Body: JSON `Change` envelope.
- `ContentType: application/json`, `DeliveryMode: 2` (persistent), `MessageId = Change.ItemID`, `Timestamp = Change.DetectedAt`.
- Headers: `feed_url`, `kind`, `schema_version`, optional `traceparent` /
  `tracestate`, optional `dlq_from_sink` / `dlq_error` / `dlq_attempts`.

Implementation notes:
- One AMQP connection + one channel per Publisher. Publishes are mutex-serialised because AMQP channels are not safe for concurrent use.
- No auto-reconnect in this version. A broker disconnect surfaces as a publish error and is handled by the sink retry+DLQ layer.

## Related

- [Choose a Sink](../choose-a-sink.md) — all drivers and the decision table.
- [Sink Wire Formats](../../reference/wire-formats.md) — on-the-wire layout.
- [Change Envelope](../../reference/change-envelope.md) — the payload body.
