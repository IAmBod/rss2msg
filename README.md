# rss2msg

Poll RSS/Atom/JSON feeds and publish changes to Postgres and Kafka (with RabbitMQ/SQS/SNS coming).

## Build & run

    make build
    ./rss2msg --config ./config.yaml serve

See `docs/superpowers/specs/2026-05-28-rss2msg-design.md` for the full design.

## Configuration

See `config.example.yaml` for a full annotated example and
`docs/superpowers/specs/2026-05-28-rss2msg-design.md` for the design notes.

## Testing

    make test                 # unit tests only
    make test-integration     # spins Postgres + Kafka via testcontainers (needs Docker)

## Quickstart

```bash
# 1. Run dependencies
docker run -d --name pg -e POSTGRES_PASSWORD=test -p 5432:5432 postgres:16-alpine
docker run -d --name kafka -p 9092:9092 confluentinc/cp-kafka:7.6.0

# 2. Set the DSN env var that the example config interpolates
export POSTGRES_DSN="postgres://postgres:test@localhost:5432/postgres?sslmode=disable"

# 3. Run a single poll cycle
./rss2msg run-once --config config.example.yaml

# 4. Inspect the published rows
psql "$POSTGRES_DSN" -c 'SELECT feed_url, item_id, kind, detected_at FROM feed_changes;'
```

## Operational notes

- **At-least-once delivery.** If one sink succeeds and another fails on the same poll, the next poll re-detects the item and re-publishes to all sinks. Downstream consumers should dedupe on `item_id` + `content_hash`.
- **Kafka `acks=none` is unsafe.** Combined with the commit-on-success model it can drop messages without state knowing. Stick with the default (`acks: all`) unless you accept the trade-off.
- **OTEL exporters require an OTLP endpoint env var.** Set `OTEL_EXPORTER_OTLP_ENDPOINT=https://collector:4317` to actually emit traces/metrics. Without it, the providers are configured but no-op.
- **Dead-letter queues.** Any sink may declare `dead_letter: <other-sink-name>`. On retry exhaustion the change is handed to the DLQ once. If no DLQ is set or the DLQ also fails, the change is dropped from this poll and re-detected on the next.
- **Running multiple instances.** Set `coordination.driver=postgres` in the
  config and ensure every instance shares the same Postgres database. Each
  instance tries `pg_try_advisory_lock` keyed by the feed URL before
  polling — losers skip that poll cycle silently. No leader election; any
  instance may pick up any feed. Crashed instances release their locks
  automatically because Postgres advisory locks die with the session.
- **AWS credentials.** SQS and SNS use the standard AWS SDK credential chain
  (`AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`, shared `~/.aws/credentials`,
  instance metadata, etc.). The config carries only `region`, `queue_url` /
  `topic_arn`, and an optional `endpoint_url` for LocalStack-style overrides.
  FIFO queues/topics are not yet supported.
- **LocalStack for development.**

      docker run -d --name ls -p 4566:4566 localstack/localstack:3.6

  Then set `endpoint_url: http://localhost:4566` on each SQS/SNS sink and
  `AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test` in the environment.
