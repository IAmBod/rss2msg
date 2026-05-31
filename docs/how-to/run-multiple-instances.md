---
title: Run Multiple Instances
type: how-to
tags: [rss2msg/docs, coordination, scaling]
summary: Gate poll cycles across horizontally-scaled instances with the memory, postgres, or redis coordinator.
updated: 2026-05-31
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
    mode: single             # single (default) | sentinel | cluster
    # --- single mode (default) ---
    url: ${REDIS_URL}        # e.g. redis://localhost:6379/0 or rediss://...
    # --- sentinel mode ---
    # sentinel:
    #   master_name: mymaster
    #   addrs: [sentinel-a:26379, sentinel-b:26379, sentinel-c:26379]
    #   username: redisuser         # authenticates to the data nodes (master/replicas)
    #   password: ${REDIS_PASSWORD} # authenticates to the data nodes (master/replicas)
    #   sentinel_username: sentuser         # authenticates to the sentinel nodes
    #   sentinel_password: ${SENTINEL_PASSWORD} # authenticates to the sentinel nodes
    #   db: 0
    # --- cluster mode (best-effort; not covered by CI integration tests) ---
    # cluster:
    #   addrs: [node-a:6379, node-b:6379, node-c:6379]
    #   username: redisuser
    #   password: ${REDIS_PASSWORD}
    # --- shared fields ---
    lock_ttl: 30s            # optional, default 30s
    renewal_interval: 10s    # optional, default = lock_ttl / 3
    tls:                     # applies to all modes; for single, requires rediss:// URL
      ca_file: /etc/ssl/redis-ca.pem
      cert_file: /etc/ssl/redis-client.pem
      key_file: /etc/ssl/redis-client.key
      server_name: redis.internal   # set this for sentinel/cluster (no URL to derive SNI from)
      insecure_skip_verify: false
```

For TLS field details on the `postgres.tls` and `redis.tls` blocks, see [Secure Connections (TLS)](secure-connections-tls.md).

| driver     | mechanism | crash recovery | notes |
| ---------- | --------- | -------------- | ----- |
| `memory`   | always grants the lease | n/a | Default. Use for single-instance deployments. |
| `postgres` | `pg_try_advisory_lock(int64(sha256(feed_url)[:8]))` per connection | automatic — advisory locks die with the session | Reuses the state DSN by default. No leader election. |
| `redis`    | `SET key token NX EX <lock_ttl>`, background renewal goroutine refreshes via CAS-checked `PEXPIRE`, release via CAS-checked `DEL`. Key = `rss2msg:coord:<hex(sha256(feed_url))>` | TTL-based — crashed instances release their leases after `lock_ttl` | Three topology modes: `single` (default), `sentinel` (tested), `cluster` (best-effort). |

## Redis coordination modes

### `mode: single` (default)

Standard single Redis node or connection via a DSN. Set `url` to a `redis://`
or `rediss://` URL. Existing configs without a `mode` field continue to work
unchanged — `single` is the default.

```yaml
coordination:
  driver: redis
  redis:
    mode: single
    url: redis://localhost:6379/0
    lock_ttl: 30s
    renewal_interval: 10s
```

For TLS, use a `rediss://` URL; the `tls:` block customizes verification
(CA, client cert, SNI override).

### `mode: sentinel`

Connects via Redis Sentinel. Sentinel is the tested and supported HA path.

```yaml
coordination:
  driver: redis
  redis:
    mode: sentinel
    sentinel:
      master_name: mymaster
      addrs: [sentinel-a:26379, sentinel-b:26379, sentinel-c:26379]
      username: redisuser           # data-node credentials (master/replicas)
      password: ${REDIS_PASSWORD}   # data-node credentials (master/replicas)
      sentinel_username: sentuser           # sentinel-node credentials
      sentinel_password: ${SENTINEL_PASSWORD} # sentinel-node credentials
      db: 0
    lock_ttl: 30s
    renewal_interval: 10s
    tls:
      ca_file: /etc/ssl/redis-ca.pem
      server_name: redis.internal   # required: no URL to derive SNI from
```

> [!warning]
> **Credential split.** `username`/`password` authenticate to the **data nodes**
> (master and replicas). `sentinel_username`/`sentinel_password` authenticate to
> the **sentinel nodes** themselves. These are separate auth targets and
> commonly have different credentials. Omitting `sentinel_username`/`sentinel_password`
> when sentinels require auth is a common misconfiguration.

> [!warning]
> **TLS and SNI.** For sentinel and cluster modes there is no URL to derive the
> SNI hostname from. If TLS is enabled and `server_name` is empty, certificate
> verification will fail against most real certificates. Always set `server_name`
> to the expected hostname when using TLS with sentinel or cluster.

### `mode: cluster`

Connects to a Redis Cluster. Cluster support is **best-effort** and is not
covered by CI integration tests.

```yaml
coordination:
  driver: redis
  redis:
    mode: cluster
    cluster:
      addrs: [node-a:6379, node-b:6379, node-c:6379]
      username: redisuser
      password: ${REDIS_PASSWORD}
    lock_ttl: 30s
    renewal_interval: 10s
    tls:
      ca_file: /etc/ssl/redis-ca.pem
      server_name: redis.internal   # required: no URL to derive SNI from
```

## Lock mechanics

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
