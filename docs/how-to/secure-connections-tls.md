---
title: Secure Connections (TLS)
type: how-to
tags: [rss2msg/docs, tls, security]
summary: Structured TLS config for Postgres (state + coordination) and Redis (coordination).
updated: 2026-05-31
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

## Related

- [Run Multiple Instances](run-multiple-instances.md) — the coordinator backends these TLS blocks secure.
- [Configuration Reference](../reference/configuration.md#state) — `state.postgres.tls` shares this field surface.
