---
title: Operational Notes
type: explanation
tags: [rss2msg/docs, operations]
summary: Delivery semantics, DLQs, multi-instance behavior, AWS creds, LocalStack, and shutdown.
updated: 2026-05-30
---

# Operational Notes

> [!note]
> **At-least-once delivery.** If one sink succeeds and another fails on the
> same poll, the next poll re-detects the item and re-publishes to *all*
> sinks. Downstream consumers should dedupe on `item_id` + `content_hash`.

> [!warning]
> **Kafka `acks: none` is unsafe.** Combined with the commit-on-success
> model it can drop messages without state knowing they were lost. Stick
> with the default (`acks: all`) unless you accept the trade-off.

- **OTEL exporters need an OTLP endpoint.** Without
  `OTEL_EXPORTER_OTLP_ENDPOINT` (or the per-signal variants), the providers
  are wired but no-op. The Prometheus exporter is a separate flag
  (`telemetry.prometheus.enabled`).
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
  instances release after `lock_ttl`.
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
