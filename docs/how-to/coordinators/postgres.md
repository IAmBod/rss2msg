---
title: Postgres coordinator
type: how-to
tags: [rss2msg/docs, coordination, scaling, postgres]
summary: Gate polling across instances with Postgres session-level advisory locks.
updated: 2026-06-09
---

# Postgres coordinator

Serialises polling across instances with a per-connection advisory lock:
`pg_try_advisory_lock(int64(sha256(feed_url)[:8]))`. Crash recovery is automatic —
advisory locks die with the session — and there is no leader election. The DSN
falls back to the state store's DSN by default, so a deployment already using
Postgres for state needs no extra connection string.

> [!warning]
> **Pair it with a shared state store.** The coordinator only serialises *polling*;
> deduplication of already-seen items lives in the
> [state store](../../reference/configuration.md#state). Set `state.driver: postgres`
> so every instance shares one dedup set — otherwise each instance keeps its own
> seen-items set and republishes items its peers already sent. Validation warns if it
> sees a distributed coordinator paired with `state.driver: sqlite`.

## Configure

```yaml
coordination:
  driver: postgres
  postgres:
    dsn: ${POSTGRES_DSN}     # falls back to state.postgres.dsn
    tls:                     # rejected if DSN has sslmode=disable
      ca_file: /etc/ssl/pg-ca.pem
      cert_file: /etc/ssl/pg-client.pem
      key_file: /etc/ssl/pg-client.key
      server_name: pg.internal
      insecure_skip_verify: false
```

For TLS field details on the `postgres.tls` block, see
[Secure Connections (TLS)](../secure-connections-tls.md).

| Property | Value |
| --- | --- |
| Mechanism | `pg_try_advisory_lock(int64(sha256(feed_url)[:8]))` per connection. |
| Crash recovery | Automatic — advisory locks die with the session. |
| Shared state store required | Yes (`state.driver: postgres`) |

## Related

- [Run Multiple Instances](../run-multiple-instances.md) — the coordinator overview, comparison table, and lock mechanics.
- [Secure Connections (TLS)](../secure-connections-tls.md) — TLS for the Postgres coordinator.
- [Configuration Reference](../../reference/configuration.md#state) — the state store, whose DSN this coordinator falls back to.
