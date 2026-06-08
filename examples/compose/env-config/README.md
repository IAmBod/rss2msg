# Configuration via environment variables

The same shape as [`minimal/`](../minimal/), but configured from the **environment**
instead of a hand-written config file — the 12-factor path you'd use when config comes
from your orchestrator (Compose, Kubernetes, ECS task definitions, …).

## Run it

```bash
docker compose up
```

## How config is supplied

rss2msg reads config from the environment two ways, and this example uses both:

1. **`RSS2MSG_*` overrides** set **scalar** fields directly, no file entry needed. The
   prefix is `RSS2MSG_` and `.` in the config path becomes `__`:

   | Env var | Sets |
   | --- | --- |
   | `RSS2MSG_LOG__LEVEL=info` | `log.level` |
   | `RSS2MSG_LOG__FORMAT=console` | `log.format` |
   | `RSS2MSG_TELEMETRY__METRICS__ENABLED=false` | `telemetry.metrics.enabled` |

   These work because each field has a built-in default. Keys with **no** registered
   default (notably `state.*`) can't be set this way and must live in the config file —
   that's why the template carries the `state:` block.

2. **`${VAR}` substitution** fills placeholders in **string** fields anywhere in the
   config tree. This is the way to drive structural blocks from the environment, because
   `RSS2MSG_*` overrides don't cover `feeds`, `sinks`, or `coordination`. The mounted
   [`config.template.yaml`](config.template.yaml) is a thin skeleton whose string values
   are `${VAR}` placeholders — `FEED_URL`, `SINK_FORMAT` — resolved from the
   `environment:` block at load time.

> **Two caveats worth knowing:**
> - Feeds and sinks are lists of structured objects, so there's no flat `RSS2MSG_*` key
>   for them — the template carries the *structure*; the environment carries the *values*.
> - `${VAR}` substitution applies only to **string** fields. The feed `interval` is a
>   duration, so it stays literal in the template (you can't env-substitute it).
>
> See [Configuration → Loading order and env vars](../../../docs/reference/configuration.md#loading-order-and-env-vars).

## Try changing it

- Point at a different feed by editing `FEED_URL` in `docker-compose.yml` (or export it
  and drop an `.env` file beside the Compose file — Compose loads `.env` automatically).
  Change the poll cadence by editing `interval:` in `config.template.yaml`.
- Switch to compact output with `SINK_FORMAT: json`.
- Flip log verbosity with `RSS2MSG_LOG__LEVEL: debug` — no file edit needed.
