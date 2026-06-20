---
title: amqp10 sink
type: how-to
tags: [rss2msg/docs, sinks, amqp10, amqp]
summary: Publish Changes to any AMQP 1.0 broker (RabbitMQ 4.x, Azure Service Bus, ActiveMQ, Solace, …) using the broker-agnostic amqp10 driver.
updated: 2026-06-20
---

# amqp10 sink

```yaml
- name: bus-main
  driver: amqp10
  amqp10:
    url: amqps://broker:5671                      # amqp:// or amqps://
    target: /queues/changes                       # node/queue/exchange address (required)
    username: ${AMQP_USER}                        # optional; else URL userinfo
    password: ${AMQP_PASS}
    tls:                                          # use an amqps:// url; adds custom CA / mTLS
      ca_file: /etc/ssl/certs/broker-ca.pem
  dead_letter: dlq-main
```

| field      | required | default | notes |
| ---------- | -------- | ------- | ----- |
| `url`      | yes      | —       | Standard AMQP URL (`amqp://` or `amqps://`). User/password inline; `${ENV}` substitution works. |
| `target`   | yes      | —       | Node/queue/topic address. For RabbitMQ see address forms below. |
| `username` | no       | (URL)   | Explicit SASL PLAIN username. Wins over URL userinfo when set. |
| `password` | no       | (URL)   | Explicit SASL PLAIN password. |
| `tls`      | no       | (off)   | Structured TLS (custom CA / mTLS / verification). When set, the sink connects with TLS; use an `amqps://` URL. See [Secure Connections (TLS)](../secure-connections-tls.md#sinks). |

## Broker-agnostic

The `amqp10` driver targets **any AMQP 1.0 broker** — it has no RabbitMQ-specific
code. The target address format and authentication details vary by broker:

| Broker              | Target address example                      | Notes |
| ------------------- | ------------------------------------------- | ----- |
| RabbitMQ 4.x        | `/queues/feed-changes`                      | Queue must exist; AMQP 1.0 plugin is built in on 4.x. Exchange routing: `/exchanges/<vhost>/<exchange>/<routing-key>`. |
| Azure Service Bus   | `<entity-name>` (queue or topic)            | Use `amqps://` URL with SAS or mTLS auth. |
| ActiveMQ / Artemis  | `queue://feed-changes`                      | Standard AMQP 1.0 addressing. |
| Solace              | `<queue-name>`                              | Solace AMQP 1.0 endpoint. |

## Auth precedence

Explicit `username`/`password` fields **win** over URL userinfo. If neither is set and
the URL has no userinfo, the sink connects with SASL ANONYMOUS.

## Publish layout

- Body: JSON-encoded `Change` envelope.
- `ContentType: application/json`, `MessageID = Change.ItemID`.
- Application properties: `feed_url`, `kind`, `schema_version`, optional
  `traceparent` / `tracestate` (W3C trace context), optional `dlq_from_sink` /
  `dlq_error` / `dlq_attempts`.

## Implementation notes

- One AMQP 1.0 connection + one session + one sender per Publisher. Sends are
  mutex-serialised because AMQP 1.0 sessions are not safe for concurrent use.
- `Publish` blocks until the broker settles the message (accept-confirmed), so a
  returned `nil` means the broker accepted the delivery.
- No auto-reconnect in this version. A broker disconnect surfaces as a publish error
  and is handled by the sink retry+DLQ layer.

## Related

- [Choose a Sink](../choose-a-sink.md) — all drivers and the decision table.
- [Secure Connections (TLS)](../secure-connections-tls.md#sinks) — custom CA / mTLS for this sink.
- [Sink Wire Formats](../../reference/wire-formats.md) — on-the-wire layout.
- [Change Envelope](../../reference/change-envelope.md) — the payload body.
