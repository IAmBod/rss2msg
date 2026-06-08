---
title: CLI
type: reference
tags: [rss2msg/docs, cli]
summary: The serve, run-once, lambda, azure-functions, validate-config, generate-config, healthcheck, and version commands, their flags, and signal handling.
updated: 2026-06-09
---

# CLI

```
rss2msg [flags] <command>

Commands
  serve              Run as a long-lived daemon; one goroutine per feed
  run-once           Poll every feed once and exit (bounded worker pool)
  lambda             Run as an AWS Lambda function; one poll cycle per invocation
  azure-functions    Run as an Azure Functions custom handler; one poll cycle per invocation
  validate-config    Parse config, dial state + each sink, exit 0/1
  generate-config    Print an annotated, runnable reference config
  healthcheck        Probe the health endpoint over HTTP, exit 0/1
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

`healthcheck` probes a running daemon's health endpoint over HTTP and exits `0`
when healthy, non-zero otherwise. It reads the same config as `serve` to find the
endpoint, rewriting a wildcard listen host (`:8080`, `0.0.0.0:8080`, `[::]:8080`)
to `127.0.0.1`. It exists so the distroless production image — which has no shell,
`curl`, or `wget` — can still define a Docker `HEALTHCHECK`; the production image
wires it in by default (see [Run with Docker](../how-to/run-with-docker.md#health-checks)).

```
rss2msg healthcheck                  Probe readiness (/readyz) and exit 0/1
rss2msg healthcheck --probe liveness Probe liveness (/healthz) instead

Flags
  --probe <kind>     Which probe to check: readiness (default), liveness, startup
  --timeout <dur>    Overall timeout for the probe request (default 2s)
```

`lambda` runs rss2msg inside the AWS Lambda runtime: it does the cold-start wiring
once, then polls every feed once per invocation (the `run-once` work, bounded by
[`runtime.run_once_concurrency`](configuration.md#runtime)). Inside Lambda with no
explicit subcommand the binary auto-starts this command, so a bare custom-runtime
`bootstrap` or a command-less container image needs no wrapper. See
[Deploy on AWS Lambda](../how-to/deploy/aws-lambda.md).

`azure-functions` (alias `azure`) runs rss2msg as an Azure Functions custom handler:
it does the cold-start wiring once, then serves an HTTP endpoint the Functions host
invokes once per trigger firing, polling every feed once per request (bounded by
[`runtime.run_once_concurrency`](configuration.md#runtime)). It listens on
`127.0.0.1` at the port the host provides via `FUNCTIONS_CUSTOMHANDLER_PORT` (default
`8080` otherwise). Inside the Functions host with no explicit subcommand the binary
auto-starts this command. See [Deploy on Azure Functions](../how-to/deploy/azure-functions.md).

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
