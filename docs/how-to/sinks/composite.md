---
title: Composite sink
type: how-to
tags: [rss2msg/docs, sinks, composite]
summary: Fan one change out to several child sinks under a single name.
updated: 2026-05-31
---

# Composite sink

A composite sink fans every change out to a list of child sinks. It is
useful when you want a single name — typically `default` — to publish to
multiple backends at once, or when you want to group a set of sinks and
reuse that group from multiple feeds.

```yaml
- name: default
  driver: composite
  composite:
    children: [pg-main, webhook]
```

## Fields

| field      | required | notes |
| ---------- | -------- | ----- |
| `children` | yes      | Ordered list of other declared sink names to deliver to. Each name must exist in the `sinks:` block. Self-reference, duplicates, and cycles are rejected at startup. A child may itself be a `composite` (nesting is allowed). |

## Dead-letter rule

A composite sink **cannot** set its own `dead_letter`. Setting
`dead_letter` on a composite is a config-validation error. Each child
carries its own `dead_letter` setting and handles retry exhaustion
independently, exactly as if the child were referenced directly by a feed.

## Delivery semantics

Children are delivered to **sequentially** in the order listed. Feed state
is committed only after every child has either succeeded or had its change
captured by that child's own dead-letter sink. If a child fails and has no
`dead_letter`, the change is left uncommitted and the entire delivery is
retried on the next poll.

There is no overlap deduplication: if the same sink is reachable by more
than one path through nested composites, it will receive the change once
per path.

## Telemetry

| counter                              | description |
| ------------------------------------ | ----------- |
| `composite_sink.publishes`           | Incremented once per change dispatched through a composite sink. |
| `composite_sink.child_deliveries`    | Incremented once per (change × child). Attributes: `sink.name` (the composite's name), `child` (the child sink's name), `outcome` (`success` \| `dlq` \| `dropped`). |

## Related

- [Choose a Sink](../choose-a-sink.md) — all drivers and the decision table.
- [Operational Notes](../../explanation/operations.md) — at-least-once delivery and DLQ behavior.
