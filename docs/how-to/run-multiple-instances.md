---
title: Run Multiple Instances
type: how-to
tags: [rss2msg/docs, coordination, scaling]
summary: Gate poll cycles across horizontally-scaled instances with the memory, postgres, or redis coordinator.
updated: 2026-05-30
---

# Run Multiple Instances

Gates which instance is allowed to poll a given feed in a given cycle, for
horizontally-scaled deployments. The default is single-instance (`memory`,
always grants the lease).

```yaml
coordination:
  driver: memory   # memory | postgres | redis ; default memory
  postgres:
    dsn: ${POSTGRES_DSN}     # falls back to state.postgres.dsn
    tls:                     # rejected if DSN has sslmode=disable
      ca_file: /etc/ssl/pg-ca.pem
      cert_file: /etc/ssl/pg-client.pem
      key_file: /etc/ssl/pg-client.key
      server_name: pg.internal
      insecure_skip_verify: false
  redis:
    url: ${REDIS_URL}        # e.g. redis://localhost:6379/0 or rediss://...
    lock_ttl: 30s            # optional, default 30s
    renewal_interval: 10s    # optional, default = lock_ttl / 3
    tls:                     # only valid when url is rediss://
      ca_file: /etc/ssl/redis-ca.pem
      cert_file: /etc/ssl/redis-client.pem
      key_file: /etc/ssl/redis-client.key
      server_name: redis.internal
      insecure_skip_verify: false
```

For TLS field details on the `postgres.tls` and `redis.tls` blocks, see [Secure Connections (TLS)](secure-connections-tls.md).

| driver     | mechanism | crash recovery | notes |
| ---------- | --------- | -------------- | ----- |
| `memory`   | always grants the lease | n/a | Default. Use for single-instance deployments. |
| `postgres` | `pg_try_advisory_lock(int64(sha256(feed_url)[:8]))` per connection | automatic — advisory locks die with the session | Reuses the state DSN by default. No leader election. |
| `redis`    | `SET key token NX EX <lock_ttl>`, background renewal goroutine refreshes via CAS-checked `PEXPIRE`, release via CAS-checked `DEL`. Key = `rss2msg:coord:<hex(sha256(feed_url))>` | TTL-based — crashed instances release their leases after `lock_ttl` | Supports `redis://` and `rediss://`. Validation rejects unparseable URLs and `lock_ttl < 1s` or `renewal_interval >= lock_ttl`. |

The pipeline calls `coord.TryAcquire(feedURL)` before each poll. On
`(release, true, nil)` it polls and `release()` runs after; on
`(nil, false, nil)` the cycle is skipped silently (no error). On
`(nil, false, err)` the cycle is skipped, a warn is logged, and the
`feed.poll.skipped{reason="coord_error"}` counter is incremented.

The release function ignores its caller's `ctx` — it uses a fresh 5 s
background context — so a canceled poll ctx (e.g. on SIGTERM) does not leak
the lease.

## Related

- [Secure Connections (TLS)](secure-connections-tls.md) — TLS for the postgres/redis coordinators.
- [Configuration Reference](../reference/configuration.md#state) — state store, which the postgres coordinator's DSN falls back to.
- [Operational Notes](../explanation/operations.md) — no-leader-election semantics and crash recovery.
