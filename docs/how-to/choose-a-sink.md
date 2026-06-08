---
title: Choose a Sink
type: how-to
tags: [rss2msg/docs, sinks]
summary: Common sink fields, dead-letter routing, and a decision table linking to each driver.
updated: 2026-05-31
---

# Choose a Sink

A non-empty list of named publishers. Each feed publishes to one or more
sinks (`feeds[].sinks: [name1, name2]`); a feed with no `sinks` list
publishes to a sink named `default` if one exists.

Common fields on every sink:

| field         | required | notes |
| ------------- | -------- | ----- |
| `name`        | yes      | Unique per config. Referenced by `feeds[].sinks` and `dead_letter`. |
| `driver`      | yes      | One of `postgres`, `kafka`, `rabbitmq`, `nats`, `sqs`, `sns`, `dynamodb`, `gcp_pubsub`, `azureservicebus`, `cosmosdb`, `dapr_pubsub`, `stdout`, `http`, `grpc`, `feed`, `composite`. |
| `dead_letter` | no       | Name of another declared sink. On retry exhaustion the change is delivered there once, with `dlq_from_sink`, `dlq_error`, and `dlq_attempts` annotations. A sink cannot be its own DLQ. |

## Drivers

| driver    | use it for                                                                               | page                            |
| --------- | ---------------------------------------------------------------------------------------- | ------------------------------- |
| postgres  | durable SQL store, queryable history                                                     | [postgres](sinks/postgres.md)   |
| kafka     | high-throughput streaming, co-partition by item                                          | [kafka](sinks/kafka.md)         |
| sqs       | AWS queue, optional FIFO ordering                                                        | [sqs](sinks/sqs.md)             |
| sns       | AWS pub/sub fan-out, optional FIFO                                                       | [sns](sinks/sns.md)             |
| dynamodb  | AWS key-value datastore; idempotent upsert change-log, consume via Streams/polling      | [dynamodb](sinks/dynamodb.md)   |
| gcp_pubsub | GCP-native pub/sub, optional ordered delivery                                           | [gcp_pubsub](sinks/gcp-pubsub.md) |
| rabbitmq  | AMQP routing (topic/direct/fanout)                                                       | [rabbitmq](sinks/rabbitmq.md)   |
| nats      | NATS subjects; core fire-and-forget or JetStream persisted+acked                         | [nats](sinks/nats.md)           |
| azureservicebus | Azure queue/topic, SAS or Azure AD auth                                            | [azureservicebus](sinks/azureservicebus.md) |
| cosmosdb  | Azure Cosmos DB (NoSQL) document store, idempotent on item id, key or Azure AD auth      | [cosmosdb](sinks/cosmosdb.md)   |
| dapr_pubsub | any broker, chosen by Dapr component YAML (one sink, 20+ brokers)                       | [dapr_pubsub](sinks/dapr-pubsub.md) |
| stdout    | local dev, debugging, ad-hoc pipelines                                                   | [stdout](sinks/stdout.md)       |
| http      | webhooks (Slack, Discord, custom receivers)                                              | [http](sinks/http.md)           |
| grpc      | typed delivery to your own gRPC `ChangeSink` server (deadlines, mTLS, streaming HTTP/2)  | [grpc](sinks/grpc.md)           |
| feed      | serve detected changes as an RSS/Atom feed over HTTP (store: memory / sqlite / postgres) | [feed](sinks/feed.md)           |
| composite | fan one change out to several child sinks under one name                                 | [composite](sinks/composite.md) |

## Related

- [Sink Wire Formats](../reference/wire-formats.md) — the on-the-wire layout.
- [Operational Notes](../explanation/operations.md) — at-least-once delivery and DLQ behavior.
