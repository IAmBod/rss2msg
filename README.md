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

