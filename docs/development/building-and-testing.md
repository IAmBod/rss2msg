---
title: Building and Testing
type: how-to
tags: [rss2msg/docs, development, testing]
summary: Build rss2msg from source and run the unit, integration, and end-to-end test suites.
updated: 2026-05-31
---

# Building and Testing

For first-time setup and your first poll, see [Getting Started](../getting-started.md). This page is for working on the codebase itself.

## Prerequisites

- **Go 1.25+** — the version declared in [`go.mod`](../../go.mod).
- **[`task`](https://taskfile.dev)** — the task runner: `go install github.com/go-task/task/v3/cmd/task@latest`, or `brew install go-task`.
- **Docker** — required only for the integration tests (testcontainers manages the container lifecycle).

## Build

```bash
task build               # compiles ./cmd/rss2msg → ./rss2msg
task clean               # removes the built binary
```

`task` lists every available target:

```bash
task                     # equivalent to `task --list`
```

## Test

```bash
task test                # unit tests: `go test -race ./...` (fast, no containers)
task test-integration    # adds -tags=integration; spins, via testcontainers:
                         #  - Postgres for state + coord/postgres + sink/postgres
                         #  - Kafka for sink/kafka
                         #  - Redis for coord/redis
                         #  - LocalStack for sink/sqs and sink/sns
```

Unit tests run with the race detector and need no external services. Integration
tests require a working Docker daemon; testcontainers boots a fresh container per
test, so the suite is slower (~30–60 s typical) but needs nothing
pre-provisioned.

End-to-end coverage lives in [`test/e2e`](../../test/e2e), which drives the full
pipeline — HTTP feed → fetcher → detector → state → Postgres sink + Kafka sink —
inside one test. AWS/LocalStack helpers live in [`test/awslocal`](../../test/awslocal).

## Static checks

```bash
task vet                 # go vet ./...
task tidy                # go mod tidy
```

## Documentation checks

Documentation links are validated by a checker that resolves every relative
markdown link under `docs/` and in the README:

```bash
bash scripts/check-doc-links.sh    # prints "OK: all relative doc links resolve"
```

Run it before opening a docs PR — see [Contributing](contributing.md).

## Related

- [Project Layout](project-layout.md) — where each package lives.
- [Contributing](contributing.md) — branch, commit, and PR conventions.
- [Getting Started](../getting-started.md) — first build and run.
