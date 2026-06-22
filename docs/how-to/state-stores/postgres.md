---
title: Postgres state store
type: how-to
tags: [rss2msg/docs, state, postgres]
summary: Persist seen-item state in a shared Postgres database for multi-instance deployments.
updated: 2026-06-22
---

# Postgres state store

A shared database, with writers serialised by the DB. The store applies its
migrations idempotently on startup. Because every instance reads and writes the same
tables, it is the state store to use whenever more than one instance runs.

## Configure

```yaml
state:
  driver: postgres
  # item_ttl: 720h            # retention since last_seen_at; 0/unset = keep forever (default)
  postgres:
    dsn: ${POSTGRES_DSN}
    # cleanup_interval: 1h    # how often to sweep expired rows (default 1h when item_ttl > 0)
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
| `state.item_ttl` | no | Retention duration since an item was last seen (e.g. `720h`). `0`/unset keeps rows forever. See [Choose a State Store — Retention and cleanup](../choose-a-state-store.md#retention-and-cleanup). |
| `postgres.cleanup_interval` | no | How often the background sweep runs. Defaults to `1h` when `item_ttl > 0`; ignored when `item_ttl` is `0`. |
| `postgres.tls.*` | no | Optional structured TLS config. Same field surface as `coordination.postgres.tls` — see [Secure Connections (TLS)](../secure-connections-tls.md) for the full table. Rejected when the DSN sets `sslmode=disable`. |

**When to use:** production, multi-instance, or when state already lives in Postgres.

A distributed coordinator (`redis`, `postgres`, or `dynamodb`) can reuse this DSN —
the Postgres coordinator falls back to `state.postgres.dsn` when its own is unset.
See [Run Multiple Instances](../run-multiple-instances.md).

## Related

- [Choose a State Store](../choose-a-state-store.md) — the overview, comparison table, and shared schema.
- [Secure Connections (TLS)](../secure-connections-tls.md) — TLS for the Postgres state store.
- [Run Multiple Instances](../run-multiple-instances.md) — sharing this store and DSN across instances.
