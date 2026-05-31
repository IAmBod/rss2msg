---
title: Choose a Sink
type: how-to
tags: [rss2msg/docs, sinks]
summary: Common sink fields, dead-letter routing, and a decision table linking to each driver.
updated: 2026-05-30
---

# Choose a Sink

A non-empty list of named publishers. Each feed publishes to one or more
sinks (`feeds[].sinks: [name1, name2]`); a feed with no `sinks` list
publishes to a sink named `default` if one exists.

Common fields on every sink:

| field         | required | notes |
| ------------- | -------- | ----- |
| `name`        | yes      | Unique per config. Referenced by `feeds[].sinks` and `dead_letter`. |
| `driver`      | yes      | One of `postgres`, `kafka`, `rabbitmq`, `sqs`, `sns`, `stdout`, `http`, `feed`. |
| `dead_letter` | no       | Name of another declared sink. On retry exhaustion the change is delivered there once, with `dlq_from_sink`, `dlq_error`, and `dlq_attempts` annotations. A sink cannot be its own DLQ. |

## Drivers

| driver   | use it for                                       | page                            |
| -------- | ------------------------------------------------ | ------------------------------- |
| postgres | durable SQL store, queryable history             | [postgres](sinks/postgres.md)   |
| kafka | high-throughput streaming, co-partition by item | [kafka](sinks/kafka.md) |
| sqs | AWS queue, optional FIFO ordering | [sqs](sinks/sqs.md) |
| sns | AWS pub/sub fan-out, optional FIFO | [sns](sinks/sns.md) |
| rabbitmq | AMQP routing (topic/direct/fanout) | [rabbitmq](sinks/rabbitmq.md) |
| stdout | local dev, debugging, ad-hoc pipelines | [stdout](sinks/stdout.md) |
| http | webhooks (Slack, Discord, custom receivers) | [http](sinks/http.md) |
| feed | serve detected changes as an RSS/Atom feed over HTTP (store: memory / sqlite / postgres) | [feed](sinks/feed.md) |

## Related

- [Sink Wire Formats](../reference/wire-formats.md) — the on-the-wire layout.
- [Operational Notes](../explanation/operations.md) — at-least-once delivery and DLQ behavior.
