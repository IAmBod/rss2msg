---
title: Operational Notes
type: explanation
tags: [rss2msg/docs, operations]
summary: Delivery semantics, DLQs, multi-instance behavior, AWS creds, LocalStack, and shutdown.
updated: 2026-06-02
---

# Operational Notes

> [!note]
> **At-least-once delivery.** A poll publishes a change to every sink *before*
> it records the item as seen — the pipeline runs all sinks, then calls
> `UpsertItem` only once they have been handled (`cmd/rss2msg/pipeline.go`). So
> duplicates are possible in two cases: (1) one sink succeeds and another fails
> on the same poll — the next poll re-detects the item and re-publishes to *all*
> sinks; (2) the process crashes or restarts between publishing and committing
> state — the item is re-detected and re-published on the next poll. There is no
> exactly-once guarantee; downstream consumers should dedupe on `item_id` +
> `content_hash`.

> [!warning]
> **Kafka `acks: none` is unsafe.** Combined with the commit-on-success
> model it can drop messages without state knowing they were lost. Stick
> with the default (`acks: all`) unless you accept the trade-off.

- **OTEL exporters need an OTLP endpoint.** Without
  `OTEL_EXPORTER_OTLP_ENDPOINT` (or the per-signal variants), the providers
  are wired but no-op. The Prometheus scrape endpoint
  (`telemetry.prometheus.enabled`) and the Graphite/Carbon push exporter
  (`telemetry.graphite.enabled`) are separate flags that work without an OTLP
  endpoint.
- **Dead-letter queues.** Any sink may declare
  `dead_letter: <other-sink-name>`. On retry exhaustion the change is
  handed to the DLQ *once*, annotated with `dlq_from_sink`, `dlq_error`,
  `dlq_attempts`. If no DLQ is set or the DLQ also fails, the change is
  dropped from this poll and re-detected on the next.
- **Running multiple instances.** Use [Run Multiple Instances](../how-to/run-multiple-instances.md) (`coordination.driver=postgres` or
  `redis`) and ensure every instance points at the same backend. No leader
  election — any instance may pick up any feed; losers skip the cycle
  silently. Postgres uses session-scoped advisory locks (auto-released on
  crash). Redis uses a TTL lease with a background renewer; crashed
  instances release after `lock_ttl`. Metrics stay per-instance: every signal
  carries `service.instance.id` (`telemetry.instance_id`, default hostname),
  which the CloudWatch and Graphite exporters add as a dimension/tag so replicas
  don't collide into one series — see [Telemetry](../reference/telemetry.md#multi-instance-deployments).
- **AWS credentials.** SQS and SNS use the AWS SDK credential chain. The
  config carries only region, queue URL / topic ARN, and an optional
  `endpoint_url` for LocalStack-style overrides. SQS FIFO queues and SNS
  FIFO topics are both supported — see the `message_group` field under
  the respective sink driver.
- **LocalStack for SQS/SNS.**
  ```bash
  docker run -d --name ls -p 4566:4566 localstack/localstack:3.6
  export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test
  ```
  Then set `endpoint_url: http://localhost:4566` on each SQS/SNS sink.
- **Shutdown.** `serve` drains in-flight publishes for up to
  `runtime.shutdown_drain_timeout` after SIGINT/SIGTERM, then forces exit.

## Related

- [Choose a Sink](../how-to/choose-a-sink.md) — declaring a `dead_letter` sink.
- [Run Multiple Instances](../how-to/run-multiple-instances.md) — coordinator crash recovery.
- [Telemetry](../reference/telemetry.md) — what to monitor.
