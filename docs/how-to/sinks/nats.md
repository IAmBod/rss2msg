---
title: NATS sink
type: how-to
tags: [rss2msg/docs, sinks, nats, jetstream]
summary: Publish Changes to a NATS subject; core fire-and-forget or JetStream persisted delivery, auth, and TLS.
updated: 2026-06-02
---

# NATS sink

```yaml
- name: nats-main
  driver: nats
  nats:
    url: nats://nats-1:4222          # comma-separate for a cluster; tls:// for TLS
    subject: feed.changes
    jetstream: false                 # true → persisted, server-acked publish
    # token: ${NATS_TOKEN}           # at most one auth group (see below)
    # username: rss
    # password: ${NATS_PASSWORD}
    # creds_file: /etc/nats/rss.creds
```

| field        | required | default | notes |
| ------------ | -------- | ------- | ----- |
| `url`        | yes      | —       | One or more comma-separated NATS server URLs (`nats://` or `tls://`). `${ENV}` substitution works. |
| `subject`    | yes      | —       | Static subject every change is published to. |
| `jetstream`  | no       | `false` | If true, publish through JetStream and wait for a server ack. The subject must already be bound to an existing stream — the sink never creates or manages streams. |
| `token`      | no       | `""`    | Token auth. |
| `username`   | no       | `""`    | User/password auth. Both or neither — validation rejects setting only one. |
| `password`   | no       | `""`    | See `username`. |
| `creds_file` | no       | `""`    | Path to a NATS user credentials file (JWT + NKey seed), e.g. for NGS / decentralized auth. |
| `tls`        | no       | (off)   | Structured TLS (custom CA / mTLS / verification). When set, the sink forces the TLS handshake (`nats.Secure`); use a `tls://` URL. See [Secure Connections (TLS)](../secure-connections-tls.md#sinks). |

At most **one** auth group may be set: `token`, `username`+`password`, or
`creds_file`. Setting more than one is a config error.

Publish layout:
- Body: JSON `Change` envelope (the message `Data`).
- Headers: `feed_url`, `kind`, `schema_version`, optional `traceparent` /
  `tracestate`, optional `dlq_from_sink` / `dlq_error` / `dlq_attempts`.

## Core vs JetStream

- **Core NATS** (`jetstream: false`, the default) is fire-and-forget pub/sub. A
  message with no connected subscriber is dropped by the server. After
  publishing, the sink flushes the connection so a broker-unreachable error
  surfaces to the retry + DLQ layer; a successful publish only means the message
  reached the server, **not** that anyone consumed it.
- **JetStream** (`jetstream: true`) publishes into a stream and waits for the
  server ack, giving durable, at-least-once delivery. The subject must already
  be captured by a stream you provision out-of-band.

Implementation notes:
- One connection per Publisher. The nats.go client is safe for concurrent
  publishes, so publishes are not serialised.
- No auto-reconnect tuning is exposed in this version; a disconnect surfaces as
  a publish/flush error and is handled by the sink retry + DLQ layer.

## Related

- [Choose a Sink](../choose-a-sink.md) — all drivers and the decision table.
- [Secure Connections (TLS)](../secure-connections-tls.md#sinks) — custom CA / mTLS for this sink.
- [Sink Wire Formats](../../reference/wire-formats.md) — on-the-wire layout.
- [Change Envelope](../../reference/change-envelope.md) — the payload body.
