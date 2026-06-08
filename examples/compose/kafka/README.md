# Kafka integration

rss2msg publishing change envelopes to a **single-node Kafka** broker (KRaft mode — no
ZooKeeper) through the [`kafka` sink](../../../docs/how-to/sinks/kafka.md). A throwaway
console-consumer service tails the topic so you can watch messages land.

## Run it

```bash
docker compose up
```

Startup order is handled for you: rss2msg and the consumer both wait on Kafka's
healthcheck before starting. Within a minute of the broker coming up you'll see JSON
change envelopes printed by the `consumer` service.

To watch just the consumer:

```bash
docker compose logs -f consumer
```

Tear down and drop Kafka's data with `docker compose down -v`.

## What's here

- **`kafka`** — `apache/kafka:3.9.0`, single-node KRaft. Auto-topic-creation is on, so
  `feed.changes` is created on first publish.
- **`rss2msg`** — `kafka` sink pointing at `kafka:9092`, topic `feed.changes`,
  `acks: all` for safe durability. See [`config.yaml`](config.yaml).
- **`consumer`** — `kafka-console-consumer.sh --from-beginning` against `feed.changes`.

## Try changing it

- **Compression** — uncomment `compression: snappy` (or `lz4` / `zstd` / `gzip`) under
  the `kafka:` sink in `config.yaml`.
- **Inspect from the host** — Kafka is published on `localhost:9092`; point your own
  `kafka-console-consumer` / `kcat` there.
- **Add a dead-letter sink** — set `dead_letter:` on the sink to capture deliveries that
  exhaust retries. See [Choose a sink](../../../docs/how-to/choose-a-sink.md).
