---
title: Secure Connections (TLS)
type: how-to
tags: [rss2msg/docs, tls, security]
summary: Structured TLS config for Postgres (state + coordination) and Redis (coordination).
updated: 2026-05-30
---

# Secure Connections (TLS)

TLS applies to the Postgres state store, the Postgres coordinator, and the Redis coordinator; the field surface is identical across them.

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

A `rediss://` URL alone gives default TLS: the system trust store for
verification, and SNI taken from the URL host. The `coordination.redis.tls`
block lets operators override that:

| field                  | default                  | notes |
| ---------------------- | ------------------------ | ----- |
| `ca_file`              | system roots             | PEM CA bundle to trust instead of system roots. |
| `cert_file`, `key_file`| (none)                   | PEM client cert + key for mTLS. Both or neither — validation rejects setting only one. |
| `server_name`          | URL host                 | Overrides the SNI / certificate verification hostname. |
| `insecure_skip_verify` | `false`                  | Disables server cert verification. Test only — logged at warn on startup. |

The TLS block is only valid when the URL uses the `rediss://` scheme;
validation rejects it for plain `redis://` so operators don't silently get
unencrypted connections.

## Related

- [Run Multiple Instances](run-multiple-instances.md) — the coordinator backends these TLS blocks secure.
- [Configuration Reference](../reference/configuration.md#state) — `state.postgres.tls` shares this field surface.
