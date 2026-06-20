# PR-1: Rename `rabbitmq` sink → `amqp091` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rename the existing AMQP 0-9-1 sink driver from `rabbitmq` to `amqp091` across code, config, validation, wiring, docs, and examples, with zero behavior change.

**Architecture:** Pure mechanical rename. The Go module dependency stays `github.com/rabbitmq/amqp091-go`; only our driver name, package, config key, and identifiers change. Because the rename spans the package, config, validation, and wiring — none of which compile in isolation — this PR is a single task whose steps land together and finish on one green build.

**Tech Stack:** Go 1.25, Cobra, Viper, `rabbitmq/amqp091-go` (unchanged).

## Global Constraints

- Go module path for the library stays `github.com/rabbitmq/amqp091-go` — do NOT rename the import target, only our alias/package.
- No installed users: breaking the `rabbitmq` driver name is acceptable (spec decision 3).
- `internal/config/example.yaml` and `examples/config.example.yaml` MUST stay byte-identical (a drift-guard test enforces this).
- Stage with explicit pathspecs only — never `git add -A`/`git add .` (Obsidian vault auto-staging hazard).
- Conventional Commits; this PR uses `refactor:`.
- `task test`, `task vet`, `task lint` must pass; `scripts/check-doc-links.sh` must print `OK: all relative doc links resolve`.

---

### Task 1: Rename the driver everywhere

**Files:**
- Rename dir: `internal/sink/rabbitmq/` → `internal/sink/amqp091/`
  - `rabbitmq.go` → `amqp091.go`; `rabbitmq_test.go` → `amqp091_test.go`; `rabbitmq_unit_test.go` → `amqp091_unit_test.go`; `tls_unit_test.go` (keep name)
- Modify: `internal/config/config.go:481-490` (`RabbitMQSinkConfig` → `AMQP091SinkConfig`, field at `:291`)
- Modify: `internal/config/validate.go:50` (driver set), `:552-564` (required-field case), `:756` (TLS-collect case)
- Modify: `internal/config/validate_test.go` (cases at `:961,979,991,1008,1025`)
- Modify: `cmd/rss2msg/wire.go:32` (import alias), `:344-358` (TLS mapper), `:539-550` (driver case)
- Modify: `docs/how-to/sinks/rabbitmq.md` → rename to `amqp091.md`; repoint links in `docs/how-to/choose-a-sink.md`, `docs/how-to/secure-connections-tls.md`, `docs/how-to/run-with-docker.md`, `docs/reference/wire-formats.md`, `docs/development/project-layout.md`, `README.md`
- Modify: `examples/config.example.yaml:224-235` and `internal/config/example.yaml:224-235`

**Interfaces:**
- Produces: package `amqp091` exporting `New(Options) (*Publisher, error)`, types `Options`, `TLSOptions`, `Publisher` (same fields/signatures as today's `rabbitmq` package — only the package name changes). Config type `AMQP091SinkConfig` with mapstructure key `amqp091`. Driver string `"amqp091"`.

- [ ] **Step 1: Move the package directory and rename its Go files**

```bash
git mv internal/sink/rabbitmq internal/sink/amqp091
git mv internal/sink/amqp091/rabbitmq.go internal/sink/amqp091/amqp091.go
git mv internal/sink/amqp091/rabbitmq_test.go internal/sink/amqp091/amqp091_test.go
git mv internal/sink/amqp091/rabbitmq_unit_test.go internal/sink/amqp091/amqp091_unit_test.go
```

- [ ] **Step 2: Rename the package clause and doc comment in every file under the package**

In each of `amqp091.go`, `amqp091_test.go`, `amqp091_unit_test.go`, `tls_unit_test.go` change `package rabbitmq` → `package amqp091`. In `amqp091.go` the leading doc comment becomes:

```go
// Package amqp091 implements the sink.Publisher interface against an AMQP 0-9-1
// broker (e.g. RabbitMQ) via amqp091-go. One connection + one channel per
// Publisher; publishes are serialised with a mutex because AMQP channels are
// NOT safe for concurrent use.
package amqp091
```

The error-message prefixes inside `amqp091.go` (e.g. `"rabbitmq sink %q: ..."`) become `"amqp091 sink %q: ..."`. Apply to all `fmt.Errorf`/`log` strings in the file. Leave the `amqp "github.com/rabbitmq/amqp091-go"` import exactly as-is.

- [ ] **Step 3: Verify the package still builds in isolation**

Run: `go build ./internal/sink/amqp091/`
Expected: builds with no output (exit 0).

- [ ] **Step 4: Rename the config type, field, and mapstructure key**

In `internal/config/config.go`, line ~291 change the field:

```go
	AMQP091         AMQP091SinkConfig         `mapstructure:"amqp091"`
```

And the type at line ~481:

```go
type AMQP091SinkConfig struct {
	URL          string        `mapstructure:"url"`
	Exchange     string        `mapstructure:"exchange"`
	ExchangeType string        `mapstructure:"exchange_type"` // direct (default) | topic | fanout | headers
	RoutingKey   string        `mapstructure:"routing_key"`
	Declare      bool          `mapstructure:"declare"`
	Durable      bool          `mapstructure:"durable"`
	Mandatory    bool          `mapstructure:"mandatory"`
	TLS          SinkTLSConfig `mapstructure:"tls"`
}
```

If the `config.go` doc comment at line ~329 lists `rabbitmq` among TLS-bearing sinks, change that word to `amqp091`.

- [ ] **Step 5: Update validation**

In `internal/config/validate.go`:
- Line ~50 driver set: replace `"rabbitmq":        {},` with `"amqp091":         {},`.
- Required-field case (~552): rename `case "rabbitmq":` → `case "amqp091":`, change `s.RabbitMQ` → `s.AMQP091` (3 occurrences), and change the three error-message prefixes `(rabbitmq %q): rabbitmq.url ...` → `(amqp091 %q): amqp091.url ...` (and the exchange_type / declare messages similarly).
- TLS-collect case (~756): `case "rabbitmq":` → `case "amqp091":`, `s.RabbitMQ.TLS` → `s.AMQP091.TLS`.

- [ ] **Step 6: Update the validation tests**

In `internal/config/validate_test.go` replace every `Driver: "rabbitmq"` with `Driver: "amqp091"`, every `RabbitMQ:` struct literal field with `AMQP091:`, and the substring asserted at line ~981 from `"rabbitmq.url"` to `"amqp091.url"`.

- [ ] **Step 7: Update the wiring**

In `cmd/rss2msg/wire.go`:
- Import alias (~32): `sinkamqp091 "github.com/iambod/rss2msg/internal/sink/amqp091"`.
- TLS mapper (~344): rename `sinkRabbitMQTLSFromConfig` → `sinkAMQP091TLSFromConfig`, its param stays `config.SinkTLSConfig`, return type `*sinkamqp091.TLSOptions`, and the comment references `amqp091`.
- Driver case (~539):

```go
	case "amqp091":
		return sinkamqp091.New(sinkamqp091.Options{
			Name:         sc.Name,
			URL:          sc.AMQP091.URL,
			Exchange:     sc.AMQP091.Exchange,
			ExchangeType: sc.AMQP091.ExchangeType,
			RoutingKey:   sc.AMQP091.RoutingKey,
			Declare:      sc.AMQP091.Declare,
			Durable:      sc.AMQP091.Durable,
			Mandatory:    sc.AMQP091.Mandatory,
			TLS:          sinkAMQP091TLSFromConfig(sc.AMQP091.TLS),
		})
```

- [ ] **Step 8: Build the whole module**

Run: `task build`
Expected: compiles to `./rss2msg` with no errors.

- [ ] **Step 9: Rename the doc and the example YAML blocks**

```bash
git mv docs/how-to/sinks/rabbitmq.md docs/how-to/sinks/amqp091.md
```

In `docs/how-to/sinks/amqp091.md`: update the frontmatter `title`, any `driver: rabbitmq` YAML to `driver: amqp091`, the config key `rabbitmq:` to `amqp091:`, and prose references to the driver name (keep references to "RabbitMQ" the product where they describe the broker, but the **driver** is `amqp091`). In every other doc listed in Files plus `README.md`, change the link `sinks/rabbitmq.md` → `sinks/amqp091.md` and any `driver: rabbitmq` mention to `driver: amqp091`.

In BOTH `examples/config.example.yaml` and `internal/config/example.yaml`, change the block at lines ~224-235 identically:

```yaml
  # - name: rmq-main
  #   driver: amqp091
  #   amqp091:
  #     url: amqp://guest:guest@rabbit-1:5672/      # or amqps:// for TLS
  #     exchange: feed.changes
  #     exchange_type: topic                        # direct (default) | topic | fanout | headers
  #     routing_key: feed.changes
  #     declare: true                               # declare the exchange at startup
  #     durable: true                               # only meaningful when declare=true
  #     tls:                                        # use an amqps:// url above; adds custom CA / mTLS
  #       ca_file: /etc/ssl/certs/rabbit-ca.pem
  #   dead_letter: dlq-main
```

- [ ] **Step 10: Verify no stale `rabbitmq` driver references remain**

Run: `grep -rn 'driver: rabbitmq\|"rabbitmq"\|RabbitMQSinkConfig\|sinkRabbitMQTLS\|s\.RabbitMQ\|sinks/rabbitmq\.md' --include='*.go' --include='*.yaml' --include='*.md' . | grep -v amqp091-go`
Expected: no output (the only allowed `rabbitmq` matches are the `amqp091-go` import path and prose naming the RabbitMQ product).

- [ ] **Step 11: Run the full local gate**

Run: `task test && task vet && task lint && bash scripts/check-doc-links.sh`
Expected: tests PASS (incl. the example-YAML drift-guard test), vet clean, lint clean, link checker prints `OK: all relative doc links resolve`.

- [ ] **Step 12: Commit**

```bash
git add internal/sink/amqp091 internal/config/config.go internal/config/validate.go internal/config/validate_test.go internal/config/example.yaml cmd/rss2msg/wire.go docs/how-to/sinks/amqp091.md docs/how-to/choose-a-sink.md docs/how-to/secure-connections-tls.md docs/how-to/run-with-docker.md docs/reference/wire-formats.md docs/development/project-layout.md examples/config.example.yaml README.md
git status   # confirm ONLY intended files staged
git commit -m "refactor(sink): rename rabbitmq driver to amqp091

No behavior change. The library dep stays amqp091-go; only the driver
name, package, config key, and identifiers change. Frees the namespace
for the new amqp10 and rabbitmq_stream sinks.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

## Self-Review

- **Spec coverage:** PR-1 of the spec (rename) — driver name, package move, config key, validation, wiring, docs, both example YAMLs, tests: all covered in Task 1 steps 1-9.
- **Placeholders:** none — every edit shows exact identifiers/paths.
- **Type consistency:** `AMQP091SinkConfig`, field `AMQP091`, key `amqp091`, alias `sinkamqp091`, mapper `sinkAMQP091TLSFromConfig`, driver string `"amqp091"` used consistently across steps 4-9 and the grep guard in step 10.
