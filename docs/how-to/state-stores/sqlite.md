---
title: SQLite state store
type: how-to
tags: [rss2msg/docs, state, sqlite]
summary: Persist seen-item state in a local SQLite file — the single-instance default.
updated: 2026-06-09
---

# SQLite state store

A single file on local disk. WAL and a busy-timeout are enabled by default, and the
store uses one connection so writes are serialised in-process. It is **not shared**
between processes or nodes.

## Configure

```yaml
state:
  driver: sqlite
  sqlite:
    path: ./rss2msg.db
```

| field | required | notes |
| --- | --- | --- |
| `sqlite.path` | yes | Filesystem path passed verbatim to the `modernc.org/sqlite` driver. `:memory:` and `?_pragma=…` query strings are accepted. |

**When to use:** single-instance deployments, local dev, edge / embedded contexts.

> [!warning]
> **Not safe for multiple instances.** Because each process keeps its own SQLite
> file, two instances do not share a seen-items set and will both republish the same
> items. Pair a horizontally-scaled deployment with the [Postgres](postgres.md) or
> [DynamoDB](dynamodb.md) state store instead; validation warns when a distributed
> coordinator is paired with `state.driver: sqlite`. See
> [Run Multiple Instances](../run-multiple-instances.md).

## Related

- [Choose a State Store](../state-stores.md) — the overview, comparison table, and shared schema.
- [Run Multiple Instances](../run-multiple-instances.md) — why SQLite is single-instance only.
