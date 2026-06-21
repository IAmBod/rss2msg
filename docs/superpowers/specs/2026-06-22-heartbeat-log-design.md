# Heartbeat log — design

Issue: [#189](https://github.com/IAmBod/rss2msg/issues/189)

## Problem

When all feeds are quiet, `rss2msg` emits no logs. Operators cannot distinguish
"the service is running, feeds simply didn't change" from "the service is wedged
or dead". The poll loop could have stalled and the logs would look identical to a
healthy-but-quiet service.

## Goal

An **opt-in** liveness signal: a single log line emitted on a fixed interval for
as long as the service runs, so a quiet-but-healthy service still produces a
steady, alertable heartbeat in the logs.

## Behaviour

- Disabled by default. When enabled, a background goroutine emits one log line
  every `interval` (default `1m`).
- The beat fires **unconditionally** on each interval — it is a pure liveness
  proof, not gated on feed activity. (The issue's "if no changes captured" framing
  describes the *value* of the beat — proving liveness during quiet periods — not a
  suppression rule. An unconditional beat is simpler, and gives operators a single
  steady rhythm to alert on a *missing* line.)
- First beat fires **after** one interval, not immediately on start.
- Emitted at **Info** level via the existing zerolog logger:
  `level=info component=heartbeat message="heartbeat: service alive"`.
- Respects the existing serve context: cancels promptly on shutdown like the other
  serve-time goroutines.
- No new metrics instrument — logs are sufficient for this signal and simpler.

## Config

New top-level section, sibling to `log` / `runtime` (consistent with the recent
top-level `retry` addition):

```yaml
heartbeat:
  enabled: false   # default off
  interval: 1m     # how often to emit the liveness line
```

- `internal/config/config.go`: add `Heartbeat HeartbeatConfig` to the top-level
  `Config`, and define `HeartbeatConfig{ Enabled bool; Interval time.Duration }`
  with `mapstructure` tags. Add defaults in `Defaults()`
  (`Enabled: false`, `Interval: time.Minute`).
- `internal/config/load.go`: register Viper defaults `heartbeat.enabled` and
  `heartbeat.interval` in `applyDefaults()`.
- `internal/config/validate.go`: `validateHeartbeat()` — when `enabled`, require
  `interval > 0`; wire it into the top-level validate path.

Env override follows the existing scheme (e.g. `RSS2MSG_HEARTBEAT_ENABLED`,
`RSS2MSG_HEARTBEAT_INTERVAL`). `interval` is a duration; both keys have registered
defaults, so env overrides work.

## Component

A small, isolated, dependency-light unit `internal/heartbeat/heartbeat.go`:

```go
// Run blocks until ctx is cancelled, calling emit() once per interval.
// The first call happens after the first full interval elapses.
func Run(ctx context.Context, interval time.Duration, emit func())
```

- Pure `select` loop over a `time.Ticker` and `ctx.Done()`.
- No logger or config dependency inside the loop — `emit` is injected, so it
  unit-tests with a tiny interval and a counter, with no real clock and no zerolog
  coupling.
- Returns immediately if `interval <= 0` (defensive; the caller already guards on
  `Enabled`).

## Wiring

In `cmd/rss2msg/serve.go`, after health starts and before `scheduler.ServeDynamic`:

```go
if cfg.Heartbeat.Enabled {
    go heartbeat.Run(ctx, cfg.Heartbeat.Interval, func() {
        tel.Logger.Info().Str("component", "heartbeat").Msg("heartbeat: service alive")
    })
}
```

Uses the same `ctx` as the other serve goroutines, so graceful shutdown already
cancels it.

## Docs / examples

- Add the `heartbeat` block to **both** `internal/config/example.yaml` and
  `examples/config.example.yaml` (must stay byte-identical — drift guard test).
- Add a short entry to the configuration reference doc describing the section.
- Run `bash scripts/check-doc-links.sh` if docs are touched.

## Testing (TDD)

- `internal/config`: defaults test (`enabled=false`, `interval=1m`); validation
  test (enabled + non-positive interval → error; enabled + positive → ok;
  disabled → ok regardless of interval).
- `internal/heartbeat`: `Run` fires `emit` ~N times over N short intervals;
  returns promptly when `ctx` is cancelled; returns immediately for
  `interval <= 0`.

## Out of scope

- Activity-gated / "quiet-only" suppression.
- Per-feed heartbeats.
- A dedicated metrics instrument for beats.

## Acceptance criteria

- With `heartbeat.enabled: true` and a short interval, the service emits a steady
  `heartbeat: service alive` line at that cadence while otherwise idle.
- Default config emits no heartbeat lines.
- The goroutine stops on shutdown without delaying drain.
- `task test`, `task vet`, `task lint` pass; example YAML files remain identical.
