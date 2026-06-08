---
title: Postgres state store
type: how-to
tags: [rss2msg/docs, state, postgres]
summary: Persist seen-item state in a shared Postgres database for multi-instance deployments.
updated: 2026-06-09
---

# Postgres state store

A shared database, with writers serialised by the DB. The store applies its
migrations idempotently on startup. Because every instance reads and writes the same
tables, it is the state store to use whenever more than one instance runs.

## Configure

```yaml
state:
  driver: postgres
  postgres:
    dsn: ${POSTGRES_DSN}
    tls:                  # rejected if DSN has sslmode=disable
      ca_file: /etc/ssl/pg-ca.pem
      cert_file: /etc/ssl/pg-client.pem
      key_file: /etc/ssl/pg-client.key
      server_name: pg.internal
      insecure_skip_verify: false
```

| field | required | notes |
| --- | --- | --- |
| `postgres.dsn` | yes | Standard `postgres://` DSN. The store applies its migrations idempotently on `New`. |
| `postgres.tls.*` | no | Optional structured TLS config. Same field surface as `coordination.postgres.tls` — see [Secure Connections (TLS)](../secure-connections-tls.md) for the full table. Rejected when the DSN sets `sslmode=disable`. |

**When to use:** production, multi-instance, or when state already lives in Postgres.

A distributed coordinator (`redis`, `postgres`, or `dynamodb`) can reuse this DSN —
the Postgres coordinator falls back to `state.postgres.dsn` when its own is unset.
See [Run Multiple Instances](../run-multiple-instances.md).

## Related

- [Choose a State Store](../choose-a-state-store.md) — the overview, comparison table, and shared schema.
- [Secure Connections (TLS)](../secure-connections-tls.md) — TLS for the Postgres state store.
- [Run Multiple Instances](../run-multiple-instances.md) — sharing this store and DSN across instances.
