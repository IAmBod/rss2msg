# rss2msg

A Go service that polls RSS / Atom / JSON feeds, detects new and updated items,
and publishes a canonical change envelope to one or more sinks (Postgres,
Kafka, RabbitMQ, SQS, SNS, Azure Service Bus). It is designed to run as a
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
- [Configure Feeds](./docs/how-to/configure-feeds.md) · [Dynamic Feed Sources](./docs/how-to/dynamic-feed-sources.md)
- [Choose a Sink](./docs/how-to/choose-a-sink.md) · drivers: [postgres](./docs/how-to/sinks/postgres.md) · [kafka](./docs/how-to/sinks/kafka.md) · [sqs](./docs/how-to/sinks/sqs.md) · [sns](./docs/how-to/sinks/sns.md) · [rabbitmq](./docs/how-to/sinks/rabbitmq.md) · [nats](./docs/how-to/sinks/nats.md) · [azureservicebus](./docs/how-to/sinks/azureservicebus.md) · [dapr_pubsub](./docs/how-to/sinks/dapr-pubsub.md) · [stdout](./docs/how-to/sinks/stdout.md) · [http](./docs/how-to/sinks/http.md) · [grpc](./docs/how-to/sinks/grpc.md)
- [Run Multiple Instances](./docs/how-to/run-multiple-instances.md)
- [Run with Docker](./docs/how-to/run-with-docker.md) · [Deploy in Production](./docs/how-to/deploy.md)
- [Secure Connections (TLS)](./docs/how-to/secure-connections-tls.md)

**Reference**
- [Configuration](./docs/reference/configuration.md) · [Change Envelope](./docs/reference/change-envelope.md) · [Wire Formats](./docs/reference/wire-formats.md) · [Telemetry](./docs/reference/telemetry.md) · [CLI](./docs/reference/cli.md)

**Understand it**
- [How It Works](./docs/explanation/how-it-works.md) · [Operational Notes](./docs/explanation/operations.md)

**Develop**
- [Building and Testing](./docs/development/building-and-testing.md) · [Contributing](./docs/development/contributing.md) · [Releasing](./docs/development/releasing.md)

---

## License

rss2msg is licensed under the [Business Source License 1.1](./LICENSE).

You may use, modify, and redistribute it freely, **including for commercial
purposes** — the one exception is that you may not offer rss2msg to third parties
as a hosted or managed "Feed-to-Message Service" without a separate commercial
license. Each released version converts to the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0)
on its Change Date (four years after release). See [`LICENSE`](./LICENSE) for the
exact terms; for commercial-licensing questions, contact info@randombullsh.it.
