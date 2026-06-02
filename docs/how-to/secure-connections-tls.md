---
title: Secure Connections (TLS)
type: how-to
tags: [rss2msg/docs, tls, security]
summary: Structured TLS config for the Postgres (state + coordination) and Redis (coordination) backends and for the network message sinks.
updated: 2026-06-02
---

# Secure Connections (TLS)

TLS applies to the Postgres state store, the Postgres coordinator, the Redis coordinator, and the network message sinks (Postgres, Kafka, RabbitMQ, HTTP, gRPC); the field surface is identical across them.

> [!warning]
> `insecure_skip_verify: true` disables server certificate verification. Test only — it is logged at `warn` on startup.

## Postgres TLS

pgx accepts TLS parameters directly in the DSN (`sslmode`, `sslrootcert`,
`sslcert`, `sslkey`). The `coordination.postgres.tls` block is a structured
alternative that keeps secrets out of the DSN string and gives the same
field surface as the Redis backend:

| field                  | default          | notes |
| ---------------------- | ---------------- | ----- |
| `ca_file`              | system roots     | PEM CA bundle to trust instead of system roots. |
| `cert_file`, `key_file`| (none)           | PEM client cert + key for mTLS. Both or neither — validation rejects setting only one. |
| `server_name`          | DSN host         | Overrides the SNI / certificate verification hostname. |
| `insecure_skip_verify` | `false`          | Disables server cert verification. Test only — logged at warn on startup. |

When a `tls` block is set, the coordinator clears pgx's plaintext
connection fallbacks so a TLS-required connection cannot silently
downgrade. Validation rejects the combination of `tls.*` with a DSN that
explicitly says `sslmode=disable` so operators don't accidentally configure
TLS knobs that would never take effect.

## Redis TLS

The `coordination.redis.tls` block is shared across all three Redis topology
modes (`single`, `sentinel`, `cluster`). Its behavior differs slightly by mode:

### `single` mode

A `rediss://` URL alone gives default TLS: the system trust store for
verification, and SNI taken from the URL host. The `tls:` block overrides
that; validation rejects it when the URL uses the plain `redis://` scheme so
operators don't silently get unencrypted connections.

### `sentinel` and `cluster` modes

TLS applies directly when the `tls:` block is present. There is no URL to
derive an SNI hostname from, so `server_name` must be set explicitly —
leaving it empty with verification enabled will fail certificate validation
against most real certificates.

| field                  | default (single)         | default (sentinel/cluster) | notes |
| ---------------------- | ------------------------ | -------------------------- | ----- |
| `ca_file`              | system roots             | system roots               | PEM CA bundle to trust instead of system roots. |
| `cert_file`, `key_file`| (none)                   | (none)                     | PEM client cert + key for mTLS. Both or neither — validation rejects setting only one. |
| `server_name`          | URL host                 | (empty — must be set)      | Overrides the SNI / certificate verification hostname. |
| `insecure_skip_verify` | `false`                  | `false`                    | Disables server cert verification. Test only — logged at warn on startup. |

## Sinks

The **postgres**, **kafka**, **rabbitmq**, **nats**, **http**, and **grpc** sinks accept the
same five knobs under a `tls:` block, plus an `enabled` flag:

```yaml
sinks:
  - name: pg
    driver: postgres
    postgres:
      dsn: "postgres://user:pass@db:5432/app"
      tls:
        ca_file: /etc/ssl/certs/db-ca.pem
        cert_file: /etc/ssl/certs/db-client.pem   # optional mTLS (both or neither)
        key_file: /etc/ssl/private/db-client.key

  - name: events
    driver: kafka
    kafka:
      brokers: ["broker:9093"]
      topic: feed-changes
      tls:
        enabled: true                             # kafka has no URL scheme; opt in here
        ca_file: /etc/ssl/certs/kafka-ca.pem

  - name: bus
    driver: rabbitmq
    rabbitmq:
      url: "amqps://user:pass@broker:5671/"       # amqps:// scheme
      tls:
        ca_file: /etc/ssl/certs/rabbit-ca.pem

  - name: events-nats
    driver: nats
    nats:
      url: "tls://nats:4222"                      # tls:// scheme
      subject: feed.changes
      tls:
        ca_file: /etc/ssl/certs/nats-ca.pem

  - name: hook
    driver: http
    http:
      url: "https://hooks.internal/feed"          # https:// scheme
      tls:
        ca_file: /etc/ssl/certs/internal-ca.pem

  - name: grpc-out
    driver: grpc
    grpc:
      target: receiver.internal:50051             # plaintext unless a tls: block is set
      tls:
        enabled: true                             # grpc has no URL scheme; opt in here
        ca_file: /etc/ssl/certs/internal-ca.pem
```

| field                  | default      | notes |
| ---------------------- | ------------ | ----- |
| `enabled`              | `false`      | Forces TLS even with no custom files (system roots). Use it for kafka, which has no URL scheme to imply TLS. |
| `ca_file`              | system roots | PEM CA bundle to trust instead of system roots. |
| `cert_file`, `key_file`| (none)       | PEM client cert + key for mTLS. Both or neither — validation rejects setting only one. |
| `server_name`          | per-connection | Overrides the SNI / verification hostname. Empty leaves the library's default (broker address / URI host / request host). |
| `insecure_skip_verify` | `false`      | Disables server cert verification. Test only — logged at warn on startup. |

A sink's `tls:` block is **active** when `enabled: true` or any field is set. When active:

- **postgres** — applies the TLS config to the pool and clears pgx's plaintext fallbacks (no silent downgrade).
- **kafka** — dials the brokers over TLS. Use `enabled: true` to turn it on with the system roots.
- **rabbitmq** — dials with TLS; use an `amqps://` URL.
- **nats** — forces the TLS handshake (`nats.Secure`); use a `tls://` URL.
- **http** — applies the TLS config to the webhook client transport; use an `https://` URL. Set `http3: true` to dial over HTTP/3 (QUIC); HTTP/3 is TLS-only, so the URL must be `https://`.
- **grpc** — dials the `target` with transport credentials. Use `enabled: true` to turn it on with the system roots; omit the block entirely for plaintext (h2c).

The **sqs** and **sns** sinks talk to AWS over HTTPS via the AWS SDK, which manages TLS
automatically — there is no `tls:` block to configure. The **feed** sink is a *server*:
configure its certificate with `feed.tls.cert_file` / `feed.tls.key_file` (see
[the feed sink how-to](sinks/feed.md)); set `feed.http3: true` to also serve
HTTP/3 (QUIC), which requires that certificate. A Postgres store backend takes
the same five client knobs under `feed.store.postgres.tls`.

## Related

- [Run Multiple Instances](run-multiple-instances.md) — the coordinator backends these TLS blocks secure.
- [Configuration Reference](../reference/configuration.md#state) — `state.postgres.tls` shares this field surface.
- [Choose a Sink](choose-a-sink.md) — all sink drivers and the decision table.
