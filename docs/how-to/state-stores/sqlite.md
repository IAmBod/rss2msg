---
title: SQLite state store
type: how-to
tags: [rss2msg/docs, state, sqlite]
summary: Persist seen-item state in a local SQLite file — the single-instance default.
updated: 2026-06-22
---

# SQLite state store

A single file on local disk. WAL and a busy-timeout are enabled by default, and the
store uses one connection so writes are serialised in-process. It is **not shared**
between processes or nodes.

## Configure

```yaml
state:
  driver: sqlite
  # item_ttl: 720h            # retention since last_seen_at; 0/unset = keep forever (default)
  sqlite:
    path: ./rss2msg.db
    # cleanup_interval: 1h    # how often to sweep expired rows (default 1h when item_ttl > 0)
```

| field | required | notes |
| --- | --- | --- |
| `sqlite.path` | yes | Filesystem path passed verbatim to the `modernc.org/sqlite` driver. `:memory:` and `?_pragma=…` query strings are accepted. |
| `state.item_ttl` | no | Retention duration since an item was last seen (e.g. `720h`). `0`/unset keeps rows forever. See [Choose a State Store — Retention and cleanup](../choose-a-state-store.md#retention-and-cleanup). |
| `sqlite.cleanup_interval` | no | How often the background sweep runs. Defaults to `1h` when `item_ttl > 0`; ignored when `item_ttl` is `0`. |

**When to use:** single-instance deployments, local dev, edge / embedded contexts.

> [!warning]
> **Not safe for multiple instances.** Because each process keeps its own SQLite
> file, two instances do not share a seen-items set and will both republish the same
> items. Pair a horizontally-scaled deployment with the [Postgres](postgres.md) or
> [DynamoDB](dynamodb.md) state store instead; validation warns when a distributed
> coordinator is paired with `state.driver: sqlite`. See
> [Run Multiple Instances](../run-multiple-instances.md).

## Related

- [Choose a State Store](../choose-a-state-store.md) — the overview, comparison table, and shared schema.
- [Run Multiple Instances](../run-multiple-instances.md) — why SQLite is single-instance only.
