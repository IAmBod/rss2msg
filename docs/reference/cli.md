---
title: CLI
type: reference
tags: [rss2msg/docs, cli]
summary: The serve, run-once, and validate-config commands, their flags, and signal handling.
updated: 2026-05-30
---

# CLI

```
rss2msg [flags] <command>

Commands
  serve              Run as a long-lived daemon; one goroutine per feed
  run-once           Poll every feed once and exit (bounded worker pool)
  validate-config    Parse config, dial state + each sink, exit 0/1

Flags
  --config <path>    Path to config file
                     (default: ./config.yaml, then /etc/rss2msg/config.yaml)
```

`serve` exits cleanly on SIGINT/SIGTERM and waits up to
[`runtime.shutdown_drain_timeout`](configuration.md) for in-flight publishes to finish.

## Related

- [Getting Started](../getting-started.md) — first run of each command.
- [Configuration Reference](configuration.md) — the config file the `--config` flag loads.
