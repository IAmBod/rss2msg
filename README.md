# rss2msg

Poll RSS/Atom/JSON feeds and publish changes to Postgres and Kafka (with RabbitMQ/SQS/SNS coming).

## Build & run

    make build
    ./rss2msg --config ./config.yaml serve

See `docs/superpowers/specs/2026-05-28-rss2msg-design.md` for the full design.
