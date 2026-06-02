---
title: Dapr pub/sub sink
type: how-to
tags: [rss2msg/docs, sinks, dapr_pubsub, dapr]
summary: Publish Changes to a Dapr pub/sub component via the local sidecar, targeting any broker Dapr supports.
updated: 2026-06-02
---

# Dapr pub/sub sink

Publishes each `Change` to a [Dapr](https://dapr.io) pub/sub component through the
local Dapr sidecar. The underlying broker — Kafka, RabbitMQ, Redis Streams, NATS,
MQTT, GCP Pub/Sub, Azure Service Bus, AWS SNS/SQS, and
[more](https://docs.dapr.io/reference/components-reference/supported-pubsub/) — is
selected by Dapr **component YAML**, not by rss2msg. One sink driver therefore
reaches every broker Dapr supports.

```yaml
- name: dapr-main
  driver: dapr_pubsub
  dapr_pubsub:
    pubsub_name: rss-pubsub        # Dapr pub/sub component name
    topic: rss.changes
    # address: localhost:50001     # sidecar gRPC endpoint; default = SDK env / localhost:50001
    # content_type: application/json
    # metadata:                    # static per-publish metadata (e.g. broker partition key)
    #   partitionKey: feeds
  dead_letter: dlq-main
```

| field          | required | notes |
| -------------- | -------- | ----- |
| `pubsub_name`  | yes      | Name of the Dapr pub/sub component (the `metadata.name` in the component YAML). |
| `topic`        | yes      | Topic to publish to. |
| `address`      | no       | Sidecar gRPC endpoint (`host:port`). Empty uses the Dapr SDK default: `DAPR_GRPC_ENDPOINT` / `DAPR_GRPC_PORT`, else `localhost:50001`. |
| `content_type` | no       | Payload content type. Default `application/json` (the format rss2msg serializes changes in). |
| `metadata`     | no       | Static key/value metadata merged into every published message. Useful for broker-specific routing (e.g. a partition key). Reserved keys below always win. |

Message data = JSON `Change` envelope. Metadata: `feed_url`, `kind`,
`schema_version`, optional `traceparent` / `tracestate`, optional DLQ
annotations (`dlq_from_sink`, `dlq_error`, `dlq_attempts`) — the same field set
as the Kafka, SQS, and GCP Pub/Sub sinks. These reserved keys take precedence
over any static `metadata` you configure. By default Dapr wraps the payload in a
[CloudEvent](https://docs.dapr.io/developing-applications/building-blocks/pubsub/pubsub-cloudevents/);
publish a raw payload by setting `rawPayload` in the component's publish metadata.

## Running the sidecar

rss2msg must run alongside a Dapr sidecar. In Kubernetes, add the Dapr
annotations to the pod (`dapr.io/enabled: "true"`, `dapr.io/app-id`); locally,
launch with `dapr run --app-id rss2msg -- ./rss2msg run`. Define the pub/sub
component (broker connection details) as a Dapr component the sidecar loads. The
sink only needs to know the component **name** and **topic** — it never holds
broker credentials itself.

## Related

- [Choose a Sink](../choose-a-sink.md) — all drivers and the decision table.
- [Sink Wire Formats](../../reference/wire-formats.md) — on-the-wire layout.
- [Change Envelope](../../reference/change-envelope.md) — the payload body.
