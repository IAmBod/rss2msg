---
title: Postgres sink
type: how-to
tags: [rss2msg/docs, sinks, postgres]
summary: Write each Change as a JSONB row; schema, table validation, and history semantics.
updated: 2026-05-30
---

# Postgres sink

```yaml
- name: pg-main
  driver: postgres
  postgres:
    dsn: ${POSTGRES_DSN}
    table: feed_changes
```

| field            | required | notes |
| ---------------- | -------- | ----- |
| `postgres.dsn`   | yes      | Standalone DSN; not required to match the state DSN. |
| `postgres.table` | yes      | Unquoted identifier (`[A-Za-z_][A-Za-z0-9_]*`, ≤ 63 chars). Validated; never interpolated raw. |
| `postgres.tls`   | no       | Structured client TLS (custom CA, mTLS, verification). When set, forces TLS and clears pgx plaintext fallbacks. See [Secure Connections (TLS)](../secure-connections-tls.md#sinks). |

Schema created on first publish (idempotent):

```sql
CREATE TABLE <table> (
    feed_url     TEXT NOT NULL,
    item_id      TEXT NOT NULL,
    kind         TEXT NOT NULL,
    payload      JSONB NOT NULL,            -- the full Change envelope
    detected_at  TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (feed_url, item_id, detected_at)
);
```

The PK includes `detected_at` so re-published changes (from re-detection
after a transient sink failure) accumulate as separate rows rather than
overwriting history. Consumers dedupe on `(feed_url, item_id, content_hash)`
from the JSONB payload.

## Related

- [Choose a Sink](../choose-a-sink.md) — all drivers and the decision table.
- [Secure Connections (TLS)](../secure-connections-tls.md#sinks) — custom CA / mTLS for this sink.
- [Sink Wire Formats](../../reference/wire-formats.md) — on-the-wire layout.
- [Change Envelope](../../reference/change-envelope.md) — the payload body.
