# Postgres (state store + sink)

rss2msg using **Postgres** for two jobs at once: as the **state store** (the dedup
memory of which items it has already seen) and as a **sink** (writing every change as a
row — the change-log / datastore pattern). See the
[`postgres` sink](../../../docs/how-to/sinks/postgres.md) and the
[Configuration reference](../../../docs/reference/configuration.md).

## Run it

```bash
docker compose up
```

rss2msg waits for Postgres to be healthy, then **auto-creates its tables** — no manual
migration step:

- **state**: `seen_items`, `feed_meta`
- **sink**: `feed_changes`

## Inspect the change log

```bash
docker compose exec postgres psql -U rss2msg -d rss2msg \
  -c 'SELECT feed_url, kind, detected_at FROM feed_changes ORDER BY detected_at DESC LIMIT 10;'
```

Postgres is also published on `localhost:5432` (user/password/db all `rss2msg`) for your
own client.

Tear down and drop the data with `docker compose down -v`. Because state is durable, a
plain `docker compose restart` (without `-v`) resumes without re-emitting already-seen
items.

## What's here

- **`postgres`** — `postgres:17-alpine` with a named volume for durability.
- **`rss2msg`** — `state.driver: postgres` and a `postgres` sink, both pointed at
  `${POSTGRES_DSN}`, which is injected via the `environment:` block rather than baked
  into the config. See [`config.yaml`](config.yaml).

## Try changing it

- **Dead-letter table** — add a second `postgres` sink (e.g. `table: feed_changes_dlq`)
  and reference it as `dead_letter:` on the primary sink.
- **TLS to Postgres** — drop `sslmode=disable` from the DSN and add a `tls:` block (see
  [Secure connections](../../../docs/how-to/secure-connections-tls.md)).
- **Scale out** — share this Postgres across replicas with a coordinator, as in
  [`../horizontal-scaling/`](../horizontal-scaling/).
