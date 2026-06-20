# Design: AMQP 1.0 + RabbitMQ Streams sinks, with `rabbitmq` → `amqp091` rename

- **Status:** Approved (design); ready for implementation planning
- **Date:** 2026-06-20
- **Scope:** Three pull requests — a rename, plus two new sink drivers

## Problem

The RabbitMQ sink only speaks AMQP 0-9-1 (`rabbitmq/amqp091-go`). We want to also
publish over **AMQP 1.0** (a broker-agnostic protocol — Azure Service Bus,
ActiveMQ, Solace, RabbitMQ 4.x, etc.) and over the **RabbitMQ Stream protocol**
(RabbitMQ-specific, binary, port 5552). These are distinct wire protocols with
distinct Go client libraries; they do not share a client with the current sink.

## Decisions (locked during brainstorming)

1. **Two new top-level drivers**, not a `protocol:` selector — keeps AMQP 1.0
   honest as broker-agnostic and gives each protocol its own config surface.
2. **Design both now, implement as separate PRs** (AGENTS.md one-PR-per-task).
3. **Rename the existing `rabbitmq` driver to `amqp091`** for symmetric,
   protocol-based naming. No installed users, so the break is free.
4. **Auth: support both** URL userinfo and explicit `username`/`password` fields
   (with `${ENV}` expansion); explicit fields win when both are present.

## Driver namespace (final)

| Driver | Protocol | Library | Default port |
|---|---|---|---|
| `amqp091` *(renamed from `rabbitmq`)* | AMQP 0-9-1 | `github.com/rabbitmq/amqp091-go` *(unchanged)* | 5672 |
| `amqp10` *(new)* | AMQP 1.0 (broker-agnostic) | `github.com/Azure/go-amqp` | 5672 |
| `rabbitmq_stream` *(new)* | RabbitMQ Stream protocol | `github.com/rabbitmq/rabbitmq-stream-go-client` | 5552 |

Package moves / additions:

- `internal/sink/rabbitmq` → `internal/sink/amqp091`
- `internal/sink/amqp10` *(new)*
- `internal/sink/rabbitmqstream` *(new)*

Config key: `sinks[].rabbitmq` → `sinks[].amqp091`.

## Extension points touched (per sink)

Each sink plugs into the same four places the existing one does:

- **Config struct** — `internal/config/config.go` (`*SinkConfig` type + field on
  the sink config).
- **Validation** — `internal/config/validate.go` (allowed-driver set + a
  per-driver required-field `case`).
- **Wiring** — `cmd/rss2msg/wire.go` (a `case` in the driver switch + a TLS mapper).
- **Implementation** — `internal/sink/<driver>/` package implementing
  `sink.Publisher` (`Name() string`, `Publish(context.Context, model.Change) error`,
  `Close() error`).

## PR breakdown

Sequenced to minimize merge conflicts; each on its own branch/worktree/PR.

### PR-1 — Rename `rabbitmq` → `amqp091` (no behavior change)

Pure rename across:

- `internal/sink/rabbitmq` → `internal/sink/amqp091` (package name, doc comment,
  type/identifier references).
- `internal/config/config.go`: `RabbitMQSinkConfig` → `AMQP091SinkConfig`, field
  `RabbitMQ` → `AMQP091`, mapstructure key `rabbitmq` → `amqp091`.
- `internal/config/validate.go`: allowed-driver map entry and the two `case
  "rabbitmq"` blocks → `amqp091`; error message prefixes.
- `cmd/rss2msg/wire.go`: `case "rabbitmq"`, the import alias, and the TLS mapper
  → `amqp091`.
- Docs: rename `docs/how-to/sinks/rabbitmq.md` → `amqp091.md`; repoint the sink
  index/README links; run `scripts/check-doc-links.sh`.
- Examples: rename the block in `examples/config.example.yaml` **and**
  `internal/config/example.yaml` (drift guard — the two must stay byte-identical).
- Tests: rename the package's tests and the `validate_test.go` cases.

Acceptance: `task test`, `task vet`, `task lint` green; the doc link checker
prints `OK`; no `rabbitmq` driver references remain except inside the new
`amqp091` package's library usage (the Go module path stays `amqp091-go`).

### PR-2 — `amqp10` sink

New package `internal/sink/amqp10` using `github.com/Azure/go-amqp`.

Config:

```yaml
- name: bus
  driver: amqp10
  amqp10:
    url: amqps://broker:5671        # amqp:// | amqps://
    target: /queues/changes         # required — node/queue/exchange address
    username: ${AMQP_USER}          # optional; falls back to URL userinfo
    password: ${AMQP_PASS}
    tls: { ca_file: ..., cert_file: ..., key_file: ..., server_name: ..., insecure_skip_verify: false }
```

Behavior:

- `New()` dials (`amqp.Dial`), opens one session and one sender to `target`.
  SASL PLAIN from explicit `username`/`password`, else from URL userinfo; no
  credentials ⇒ anonymous. TLS via the shared `SinkTLSConfig` shape (mTLS
  supported, `insecure_skip_verify` logged at warn — mirror the amqp091 sink).
- `Publish` builds an `*amqp.Message`: JSON body, `Properties.ContentType =
  application/json`, `Properties.MessageID = change.ItemID`, and
  `ApplicationProperties` carrying `feed_url`, `kind`, `schema_version`, the
  `dlq_*` trio when set, and `traceparent`/`tracestate` from the OTel
  propagator. `sender.Send(ctx, msg, nil)` waits for the disposition, so a
  returned `nil` means accepted; sender serialized with a mutex.
- `Close()` closes sender → session → conn.

Validation: `url` and `target` required.

### PR-3 — `rabbitmq_stream` sink

New package `internal/sink/rabbitmqstream` using
`github.com/rabbitmq/rabbitmq-stream-go-client`.

Config:

```yaml
- name: stream
  driver: rabbitmq_stream
  rabbitmq_stream:
    uris: ["rabbitmq-stream://host:5552/%2f"]  # or a single `url:`
    stream: changes                  # required
    username: ${RMQ_USER}            # optional; else URI userinfo
    password: ${RMQ_PASS}
    declare: true                    # create the stream if absent
    max_age: 168h                    # optional retention (declare only)
    max_length_bytes: 5368709120     # optional retention (declare only)
    tls: { ... }                     # same SinkTLSConfig shape
```

Behavior:

- `New()` builds a `stream.Environment` from the URI(s) (or `url`), applying
  explicit `username`/`password` over URI userinfo and TLS. When `declare=true`,
  create the stream with the optional retention options (idempotent). Open one
  `Producer` on `stream`.
- `Publish` encodes the same JSON body + AMQP 1.0 message properties /
  application properties as the amqp10 sink (streams use AMQP 1.0 message
  encoding). It sends through the async producer and **blocks on a confirmation**
  correlated by publishingID, returning the broker's outcome (or `ctx`
  cancellation) as the error. The confirmation-correlation map is mutex-guarded.
- `Close()` closes the producer and the environment.

Validation: `stream` required, plus at least one of `uris` / `url`.

**Out of scope (future work):** publisher-side **deduplication** (stream
`producer_name` + monotonic numeric publishingID). `model.Change.ItemID` is a
non-monotonic string; correct dedup needs a separate design. v1 publishes
without dedup.

## Shared conventions

Mirror the existing kafka/sqs/sns/amqp091 sinks exactly:

- Body: `json.Marshal(model.Change)`, `content-type: application/json`.
- Metadata keys: `feed_url`, `kind`, `schema_version`; `dlq_from_sink`,
  `dlq_error`, `dlq_attempts` when `change.DLQFromSink != ""`; W3C
  `traceparent`/`tracestate` injected via `otel.GetTextMapPropagator()`.
- `Publish` is synchronous and confirmed: `nil` is returned only after the
  broker accepts the message.

## Testing

- **Unit (`task test`, no containers):** per sink — option/required-field
  validation, auth-precedence (explicit fields beat URL userinfo), and
  message-mapping (properties + trace-context). TDD: failing test first.
- **Integration (`-tags=integration`, testcontainers, Docker):**
  - amqp091 (unchanged) + amqp10 against `rabbitmq:4-management` (4.x speaks
    AMQP 1.0 natively).
  - rabbitmq_stream against a RabbitMQ container with the `rabbitmq_stream`
    plugin enabled and port 5552 exposed.
  - Both new suites join the existing **rabbitmq CI shard** (the package-sharded
    integration matrix that fixed runner disk exhaustion).
- **Deps:** `task tidy` after adding the two libraries.
- **Docs:** `scripts/check-doc-links.sh` must print `OK` after the doc changes.

## Docs & examples

- Rename `docs/how-to/sinks/rabbitmq.md` → `amqp091.md` (PR-1); add
  `docs/how-to/sinks/amqp10.md` (PR-2) and `docs/how-to/sinks/rabbitmq-stream.md`
  (PR-3), each with the standard frontmatter and `## Related` footer.
- Update the sinks index/README sink list in each PR.
- Add the new config blocks to **both** `examples/config.example.yaml` and
  `internal/config/example.yaml`, keeping them byte-identical (drift guard test).

## Acceptance criteria (all PRs)

- `task test`, `task vet`, `task lint` pass.
- `task test-integration` exercised for the touched sink (Docker) — or explicitly
  noted if skipped.
- Docs/config examples updated; `scripts/check-doc-links.sh` prints `OK`.
- Only intended files staged (Obsidian vault auto-staging hazard).
- Conventional Commit messages (`refactor:` for PR-1, `feat:` for PR-2/PR-3).
