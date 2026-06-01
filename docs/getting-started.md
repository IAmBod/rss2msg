---
title: Getting Started
type: tutorial
tags: [rss2msg/docs, quickstart]
summary: Build rss2msg and run your first one-shot and daemon polls against a feed.
updated: 2026-05-30
---

# Getting Started

## Build

```bash
task build               # produces ./rss2msg
task test                # unit tests (fast, no containers)
task test-integration    # adds testcontainers (Postgres, Kafka, Redis, LocalStack)
task vet                 # go vet ./...
task tidy                # go mod tidy
task                     # list all tasks
```

Requires Go 1.22+ and [`task`](https://taskfile.dev) (`go install github.com/go-task/task/v3/cmd/task@latest`, or `brew install go-task`). Integration tests require a working Docker daemon.

## First run

```bash
# 1. Build
task build

# 2. Run Postgres + Kafka locally (skip what you don't need)
docker run -d --name pg    -e POSTGRES_PASSWORD=test -p 5432:5432 postgres:16-alpine
docker run -d --name kafka -p 9092:9092 confluentinc/cp-kafka:7.6.0

# 3. Tell the example config where to find Postgres
export POSTGRES_DSN="postgres://postgres:test@localhost:5432/postgres?sslmode=disable"

# 4. One-shot: poll every feed once, publish, exit
./rss2msg run-once --config examples/config.example.yaml

# 5. Inspect what landed in Postgres
psql "$POSTGRES_DSN" -c 'SELECT feed_url, item_id, kind, detected_at FROM feed_changes;'

# 6. Or run as a daemon
./rss2msg serve --config examples/config.example.yaml
```

For SQS/SNS try LocalStack:

```bash
docker run -d --name ls -p 4566:4566 localstack/localstack:3.6
export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test
# add endpoint_url: http://localhost:4566 under each sqs/sns sink
```

For multi-instance coordination, see [Run Multiple Instances](how-to/run-multiple-instances.md).

## Related

- [CLI](reference/cli.md) — every command and flag.
- [Configure Feeds](how-to/configure-feeds.md) — replace the example feeds with your own.
- [Choose a Sink](how-to/choose-a-sink.md) — send changes somewhere other than the example.
