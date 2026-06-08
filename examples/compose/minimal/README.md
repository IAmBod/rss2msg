# Minimal

The smallest useful rss2msg deployment: **one container**, a **`stdout` sink**, and
config supplied from a **mounted file**. It polls a public feed and prints a change
envelope for every new or updated item.

## Run it

```bash
docker compose up
```

On the first poll the state store is empty, so every current item in the feed is
emitted as `new`. After that you'll only see genuinely new or changed items. Stop with
`Ctrl-C` (or `docker compose down` if you ran with `-d`).

## What's here

- [`config.yaml`](config.yaml) — bind-mounted to `/etc/rss2msg/config.yaml`, where the
  binary looks for config by default. One feed, one `stdout` sink, SQLite state on a
  writable tmpfs.
- [`docker-compose.yml`](docker-compose.yml) — a single `rss2msg` service running the
  published image with `serve`.

## Try changing it

- **Add a feed** — append another entry under `feeds:` in `config.yaml`.
- **Compact output** — set `stdout.format: json` for one NDJSON line per change.
- **One-shot instead of a daemon** — override the command to poll once and exit:

  ```bash
  docker compose run --rm rss2msg run-once
  ```

- **Validate before running** — `docker compose run --rm rss2msg validate-config`.

## Next steps

- Send changes to a real broker → [`../kafka/`](../kafka/), [`../rabbitmq/`](../rabbitmq/).
- Persist to a database → [`../postgres/`](../postgres/).
- Configure without a file → [`../env-config/`](../env-config/).

See the [Configuration reference](../../../docs/reference/configuration.md) for every
field, and [Choose a sink](../../../docs/how-to/choose-a-sink.md) for the full driver list.
