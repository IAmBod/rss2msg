---
title: Stdout sink
type: how-to
tags: [rss2msg/docs, sinks, stdout]
summary: Write one Change per line to stdout/stderr for local dev and ad-hoc pipelines.
updated: 2026-05-30
---

# Stdout sink

Writes one Change per line to stdout (or stderr). Intended for local
development, debugging, and ad-hoc pipelines:

```bash
./rss2msg run-once --config debug.yaml | jq 'select(.kind == "new") | .title'
```

```yaml
- name: out
  driver: stdout
  stdout:
    target: stdout   # stdout (default) | stderr
    format: json     # json (default, NDJSON) | pretty (indented; not line-parseable)
```

| field    | required | default  | notes |
| -------- | -------- | -------- | ----- |
| `target` | no       | `stdout` | `stdout` \| `stderr`. |
| `format` | no       | `json`   | `json` writes one Change envelope per line (NDJSON). `pretty` indents with 2 spaces — human-readable in a terminal, but no longer one-record-per-line. |

Concurrent publishes from different feeds are mutex-serialised so records
never interleave bytes mid-line. Note that on a daemon whose `log.format`
is also `json`, the stdout sink output and the zerolog records share the
same pipe; downstream consumers can filter (e.g. by presence of
`schema_version` for Changes vs. `level` for log records).

## Related

- [Choose a Sink](../choose-a-sink.md) — all drivers and the decision table.
- [Sink Wire Formats](../../reference/wire-formats.md) — on-the-wire layout.
- [Change Envelope](../../reference/change-envelope.md) — the payload body.
