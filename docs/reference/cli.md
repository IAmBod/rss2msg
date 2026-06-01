---
title: CLI
type: reference
tags: [rss2msg/docs, cli]
summary: The serve, run-once, validate-config, generate-config, and version commands, their flags, and signal handling.
updated: 2026-06-01
---

# CLI

```
rss2msg [flags] <command>

Commands
  serve              Run as a long-lived daemon; one goroutine per feed
  run-once           Poll every feed once and exit (bounded worker pool)
  validate-config    Parse config, dial state + each sink, exit 0/1
  generate-config    Print an annotated, runnable reference config
  version            Print version, commit, and build date

Flags
  --config <path>    Path to config file
                     (default: ./config.yaml, then /etc/rss2msg/config.yaml)
```

`generate-config` (alias `gen-config`) writes a complete, fully-annotated
reference configuration — the same content as
[`examples/config.example.yaml`](../../examples/config.example.yaml) — so you can
bootstrap your own `config.yaml`:

```
rss2msg generate-config            Print the reference config to stdout
rss2msg generate-config > config.yaml
rss2msg generate-config -o config.yaml    Write to a file (refuses to clobber)
rss2msg generate-config -o config.yaml -f Overwrite an existing file

Flags
  -o, --output <path>   Write to this file instead of stdout
  -f, --force           Overwrite the output file if it already exists
```

The emitted config is runnable as-is and passes `validate-config` unchanged.

`serve` exits cleanly on SIGINT/SIGTERM and waits up to
[`runtime.shutdown_drain_timeout`](configuration.md#runtime) for in-flight publishes to finish.

`version` reports the build metadata stamped in by the release pipeline (version, git
commit, build date, plus the Go and OS/arch). On a plain `go build` it prints
`dev`/`none`/`unknown`; release binaries carry the real values. See
[Releasing](../development/releasing.md).

## Related

- [Getting Started](../getting-started.md) — first run of each command.
- [Configuration Reference](configuration.md) — the config file the `--config` flag loads.
- [Releasing](../development/releasing.md) — how version metadata is produced.
