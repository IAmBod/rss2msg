---
title: Project Layout
type: reference
tags: [rss2msg/docs, development, architecture]
summary: The package map — what each cmd/ and internal/ package is responsible for.
updated: 2026-05-31
---

# Project Layout

Module: `github.com/iambod/rss2msg`. For the runtime data flow these packages
implement, see [How It Works](../explanation/how-it-works.md).

## Entry point

| Path | Responsibility |
| --- | --- |
| [`cmd/rss2msg`](../../cmd/rss2msg) | CLI entry point (`package main`). Wires config → telemetry → state → sinks → scheduler and defines the `serve`, `run-once`, and `validate-config` commands. See [CLI](../reference/cli.md). |

## Internal packages

| Path | Responsibility |
| --- | --- |
| [`internal/config`](../../internal/config) | Loads and validates configuration (defaults → file → `RSS2MSG_*` env → `${VAR}` substitution). See [Configuration Reference](../reference/configuration.md). |
| [`internal/feed`](../../internal/feed) | Feed fetching (conditional GET, parsing) and the `Detector` that classifies items new/updated/unchanged by content hash. |
| [`internal/feedsource`](../../internal/feedsource) | `Source` interface, `FeedSpec` schema, the `Poll` helper, the in-memory `static` source, and the precedence-merge `aggregator`. Pull-source backends live in subpackages: `feedsource/sources/{file,http,postgres,kubernetes}`. See [Load Feeds Dynamically](../how-to/load-feeds-dynamically.md). |
| [`internal/model`](../../internal/model) | The `Change` envelope — the canonical published message. See [Change Envelope](../reference/change-envelope.md). |
| [`internal/state`](../../internal/state) | `Store` interface plus `ItemState` / `FeedMeta` types (the `seen_items` + `feed_meta` tables). Backends: `state/postgres`, `state/sqlite`. |
| [`internal/coord`](../../internal/coord) | `Coordinator` interface that gates polling across instances. Backends: `coord/memory`, `coord/postgres`, `coord/redis`. See [Run Multiple Instances](../how-to/run-multiple-instances.md). |
| [`internal/sink`](../../internal/sink) | `Publisher` abstraction and the `RetryingPublisher` wrapper. Driver backends: `sink/{postgres,kafka,amqp091,sqs,sns,stdout,http}`. See [Choose a Sink](../how-to/choose-a-sink.md). |
| [`internal/retry`](../../internal/retry) | Retry policy (`Config`/`Result`) — exponential backoff with jitter applied per publish. See the `retry` block in [Configuration Reference](../reference/configuration.md). |
| [`internal/scheduler`](../../internal/scheduler) | Drives execution: `RunOnce` (bounded worker pool) for `run-once`, the per-feed scheduling loop for `serve`, and `ServeDynamic` which reconciles the feed set from `feedsource` (SIGHUP / file-watch reload). |
| [`internal/telemetry`](../../internal/telemetry) | zerolog + OpenTelemetry setup (`Telemetry`, `Instruments`). See [Telemetry](../reference/telemetry.md). |

## Tests

| Path | Responsibility |
| --- | --- |
| [`test/e2e`](../../test/e2e) | End-to-end test driving the full pipeline in one process. |
| [`test/awslocal`](../../test/awslocal) | LocalStack helpers for the SQS/SNS sinks. |

Most packages also carry colocated `*_test.go` unit tests; integration tests are
guarded by the `integration` build tag — see [Building and Testing](building-and-testing.md).

## Related

- [Building and Testing](building-and-testing.md) — build and run the suites.
- [How It Works](../explanation/how-it-works.md) — how these packages cooperate at runtime.
- [Contributing](contributing.md) — conventions for changing them.
