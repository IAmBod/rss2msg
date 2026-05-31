# rss2msg

A Go service that polls RSS / Atom / JSON feeds, detects new and updated items,
and publishes a canonical change envelope to one or more sinks (Postgres,
Kafka, RabbitMQ, SQS, SNS). It is designed to run as a
long-lived daemon (`serve`) or as a single-shot job (`run-once`), and it can
scale to multiple instances behind a shared coordinator (Postgres advisory
locks or Redis lease) without leader election.

> **Architecture:** `feeds → fetcher/detector → state store → sinks`, with a coordinator gating poll cycles across instances. See the interactive [pipeline canvas](./docs/explanation/architecture.canvas) (opens in Obsidian).

---

## Documentation

Full documentation lives in [`docs/`](./docs/index.md). Start there, or jump in:

**Get started**
- [Getting Started](./docs/getting-started.md) — build and run your first poll.

**How-to**
- [Configure Feeds](./docs/how-to/configure-feeds.md)
- [Choose a Sink](./docs/how-to/choose-a-sink.md) · drivers: [postgres](./docs/how-to/sinks/postgres.md) · [kafka](./docs/how-to/sinks/kafka.md) · [sqs](./docs/how-to/sinks/sqs.md) · [sns](./docs/how-to/sinks/sns.md) · [rabbitmq](./docs/how-to/sinks/rabbitmq.md) · [stdout](./docs/how-to/sinks/stdout.md) · [http](./docs/how-to/sinks/http.md)
- [Run Multiple Instances](./docs/how-to/run-multiple-instances.md)
- [Secure Connections (TLS)](./docs/how-to/secure-connections-tls.md)

**Reference**
- [Configuration](./docs/reference/configuration.md) · [Change Envelope](./docs/reference/change-envelope.md) · [Wire Formats](./docs/reference/wire-formats.md) · [Telemetry](./docs/reference/telemetry.md) · [CLI](./docs/reference/cli.md)

**Understand it**
- [How It Works](./docs/explanation/how-it-works.md) · [Operational Notes](./docs/explanation/operations.md)
