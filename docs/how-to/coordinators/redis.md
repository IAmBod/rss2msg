---
title: Redis coordinator
type: how-to
tags: [rss2msg/docs, coordination, scaling, redis]
summary: Gate polling across instances with a TTL-based Redis lock, in single, sentinel, or cluster topology.
updated: 2026-06-09
---

# Redis coordinator

Serialises polling across instances with a TTL-based lock: `SET key token NX EX
<lock_ttl>`, a background renewal goroutine refreshes it via CAS-checked `PEXPIRE`,
and release is a CAS-checked `DEL`. The key is
`rss2msg:coord:<hex(sha256(feed_url))>`. Crash recovery is TTL-based — a crashed
instance releases its leases after `lock_ttl`. There is no leader election.

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
  driver: redis
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

For TLS field details on the `redis.tls` block, see
[Secure Connections (TLS)](../secure-connections-tls.md).

| Property | Value |
| --- | --- |
| Mechanism | `SET key token NX EX <lock_ttl>`; renewal via CAS-checked `PEXPIRE`; release via CAS-checked `DEL`. |
| Crash recovery | TTL-based — crashed instances release their leases after `lock_ttl`. |
| Shared state store required | Yes (`state.driver: postgres`) |

## Topology modes

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

## Related

- [Run Multiple Instances](../run-multiple-instances.md) — the coordinator overview, comparison table, and lock mechanics.
- [Secure Connections (TLS)](../secure-connections-tls.md) — TLS for the Redis coordinator.
- [Configuration Reference](../../reference/configuration.md#state) — the state store.
