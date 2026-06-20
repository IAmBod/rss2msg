# PR-3: `rabbitmq_stream` sink Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `driver: rabbitmq_stream` sink that publishes `model.Change` JSON to a RabbitMQ Stream (native stream protocol, port 5552) via `github.com/rabbitmq/rabbitmq-stream-go-client`, with synchronous publish-confirmation, optional stream declaration with retention, auth, and TLS.

**Architecture:** New package `internal/sink/rabbitmqstream`. `New` builds a `stream.Environment` from URI(s) (or `url`), optionally declares the stream with retention, and opens one `Producer`. `Publish` serializes under a mutex and sends exactly one in-flight message, blocking on the producer's confirmation channel so a returned `nil` means the broker confirmed it. Message construction is a pure helper (`buildMessage`) for unit testing. Publisher-side deduplication is explicitly out of scope (see Constraints).

**Tech Stack:** Go 1.25, `github.com/rabbitmq/rabbitmq-stream-go-client` (packages `pkg/stream` and `pkg/amqp`), zerolog, OpenTelemetry propagation.

## Global Constraints

- Builds on PR-1; branch off `main` after PR-1 merges. Independent of PR-2.
- Auth "support both": explicit `username`/`password` win over URI userinfo.
- Metadata keys identical to the other sinks: `feed_url`, `kind`, `schema_version`, `dlq_*` trio when set, `traceparent`/`tracestate`.
- `Publish` returns `nil` only after a broker confirmation; serialize one in-flight message per send (simple + correct for RSS volumes). Batching/pipelining is a future optimization.
- **Out of scope (future work):** publisher-side dedup (`producer_name` + monotonic numeric publishingID). `model.Change.ItemID` is a non-monotonic string; v1 publishes without dedup.
- `examples/config.example.yaml` and `internal/config/example.yaml` stay byte-identical.
- Explicit-pathspec staging only. Conventional Commits (`feat:`). `task test`, `task vet`, `task lint`, `scripts/check-doc-links.sh` must pass. `task tidy` after adding the dep. Integration test joins the existing **rabbitmq CI shard**.

## File Structure

- `internal/sink/rabbitmqstream/rabbitmqstream.go` — `Options`, `TLSOptions`, `Publisher`, `New`, `Publish`, `Close`, helpers `buildMessage`/`buildEnvOptions`/`buildTLSConfig`.
- `internal/sink/rabbitmqstream/rabbitmqstream_unit_test.go` — validation + message mapping (no broker).
- `internal/sink/rabbitmqstream/rabbitmqstream_test.go` — integration (`//go:build integration`).
- `internal/config/config.go`, `internal/config/validate.go`, `cmd/rss2msg/wire.go`.
- `docs/how-to/sinks/rabbitmq-stream.md`; sink index/README; both example YAMLs.

---

### Task 1: Config, validation, and wiring for `rabbitmq_stream`

**Files:**
- Modify: `internal/config/config.go`, `internal/config/validate.go`, `internal/config/validate_test.go`, `cmd/rss2msg/wire.go`

**Interfaces:**
- Produces: `config.RabbitMQStreamSinkConfig{ URIs []string; URL, Stream, Username, Password string; Declare bool; MaxAge time.Duration; MaxLengthBytes int64; TLS SinkTLSConfig }`, key `rabbitmq_stream`; driver string `"rabbitmq_stream"`; wire case calling `sinkrabbitmqstream.New(ctx, ...)`.

- [ ] **Step 1: Write the failing validation test**

In `internal/config/validate_test.go`:

```go
func TestValidateRabbitMQStreamRequiresStreamAndURI(t *testing.T) {
	c := minimalValidConfig()
	c.Sinks = append(c.Sinks, SinkConfig{Name: "st", Driver: "rabbitmq_stream"})
	if _, err := c.Validate(); err == nil || !strings.Contains(err.Error(), "rabbitmq_stream.stream") {
		t.Fatalf("want stream required error, got %v", err)
	}
	c.Sinks[len(c.Sinks)-1].RabbitMQStream = RabbitMQStreamSinkConfig{Stream: "changes"}
	if _, err := c.Validate(); err == nil || !strings.Contains(err.Error(), "rabbitmq_stream.uris") {
		t.Fatalf("want uris/url required error, got %v", err)
	}
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./internal/config/ -run TestValidateRabbitMQStream -v`
Expected: FAIL — `RabbitMQStreamSinkConfig` undefined.

- [ ] **Step 3: Add the config type and field**

In `internal/config/config.go`, add to `SinkConfig`:

```go
	RabbitMQStream  RabbitMQStreamSinkConfig  `mapstructure:"rabbitmq_stream"`
```

And the type (`time` is already imported by config.go):

```go
// RabbitMQStreamSinkConfig configures the RabbitMQ Stream protocol sink.
type RabbitMQStreamSinkConfig struct {
	URIs           []string      `mapstructure:"uris"`             // rabbitmq-stream://... (or set url)
	URL            string        `mapstructure:"url"`             // single-URI shorthand
	Stream         string        `mapstructure:"stream"`          // target stream (required)
	Username       string        `mapstructure:"username"`        // optional; overrides URI userinfo
	Password       string        `mapstructure:"password"`        // optional; overrides URI userinfo
	Declare        bool          `mapstructure:"declare"`         // create the stream if absent
	MaxAge         time.Duration `mapstructure:"max_age"`         // retention; declare only
	MaxLengthBytes int64         `mapstructure:"max_length_bytes"` // retention; declare only
	TLS            SinkTLSConfig `mapstructure:"tls"`
}
```

- [ ] **Step 4: Register driver + required-field + TLS-collect**

In `internal/config/validate.go`: add `"rabbitmq_stream":  {},` to `knownSinkDrivers`. Required-field case:

```go
		case "rabbitmq_stream":
			if strings.TrimSpace(s.RabbitMQStream.Stream) == "" {
				return *warnings, fmt.Errorf("sinks[%d] (rabbitmq_stream %q): rabbitmq_stream.stream is required", i, s.Name)
			}
			if len(s.RabbitMQStream.URIs) == 0 && strings.TrimSpace(s.RabbitMQStream.URL) == "" {
				return *warnings, fmt.Errorf("sinks[%d] (rabbitmq_stream %q): rabbitmq_stream.uris or rabbitmq_stream.url is required", i, s.Name)
			}
```

TLS-collect switch: `case "rabbitmq_stream":` → `stls = s.RabbitMQStream.TLS`.

- [ ] **Step 5: Run validation test to verify it passes**

Run: `go test ./internal/config/ -run TestValidateRabbitMQStream -v`
Expected: PASS.

- [ ] **Step 6: Add the wiring**

In `cmd/rss2msg/wire.go` add import `sinkrabbitmqstream "github.com/iambod/rss2msg/internal/sink/rabbitmqstream"`, a TLS mapper `sinkRabbitMQStreamTLSFromConfig` (same shape as the others, returning `*sinkrabbitmqstream.TLSOptions`), and the case:

```go
	case "rabbitmq_stream":
		return sinkrabbitmqstream.New(ctx, sinkrabbitmqstream.Options{
			Name:           sc.Name,
			URIs:           sc.RabbitMQStream.URIs,
			URL:            sc.RabbitMQStream.URL,
			Stream:         sc.RabbitMQStream.Stream,
			Username:       sc.RabbitMQStream.Username,
			Password:       sc.RabbitMQStream.Password,
			Declare:        sc.RabbitMQStream.Declare,
			MaxAge:         sc.RabbitMQStream.MaxAge,
			MaxLengthBytes: sc.RabbitMQStream.MaxLengthBytes,
			TLS:            sinkRabbitMQStreamTLSFromConfig(sc.RabbitMQStream.TLS),
		})
```

- [ ] **Step 7: Commit**

```bash
git add internal/config/config.go internal/config/validate.go internal/config/validate_test.go cmd/rss2msg/wire.go
git commit -m "feat(sink): wire rabbitmq_stream config and validation

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: The `rabbitmq_stream` Publisher

**Files:**
- Create: `internal/sink/rabbitmqstream/rabbitmqstream.go`
- Create: `internal/sink/rabbitmqstream/rabbitmqstream_unit_test.go`

**Interfaces:**
- Consumes: `model.Change`; `Options` from Task 1's wire case.
- Produces: `New(ctx context.Context, opts Options) (*Publisher, error)`; `Options{ Name string; URIs []string; URL, Stream, Username, Password string; Declare bool; MaxAge time.Duration; MaxLengthBytes int64; TLS *TLSOptions }`; `TLSOptions` (same five fields); `(*Publisher).Name() string`, `Publish(context.Context, model.Change) error`, `Close() error`.

- [ ] **Step 1: Add the dependency**

```bash
go get github.com/rabbitmq/rabbitmq-stream-go-client@latest
task tidy
```

- [ ] **Step 2: Write the failing unit tests**

`internal/sink/rabbitmqstream/rabbitmqstream_unit_test.go`:

```go
package rabbitmqstream

import (
	"testing"
	"time"

	streamamqp "github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"

	"github.com/iambod/rss2msg/internal/model"
)

func TestNewRejectsMissingFields(t *testing.T) {
	t.Parallel()
	if _, err := New(t.Context(), Options{URIs: []string{"rabbitmq-stream://h:5552/"}}); err == nil {
		t.Fatal("want error for missing name")
	}
	if _, err := New(t.Context(), Options{Name: "x", URIs: []string{"rabbitmq-stream://h:5552/"}}); err == nil {
		t.Fatal("want error for missing stream")
	}
	if _, err := New(t.Context(), Options{Name: "x", Stream: "s"}); err == nil {
		t.Fatal("want error for missing uris/url")
	}
}

func TestBuildMessageMapsChange(t *testing.T) {
	t.Parallel()
	ch := model.Change{
		FeedURL:       "https://example.com/feed",
		Kind:          model.ChangeKindNew,
		SchemaVersion: 1,
		ItemID:        "item-7",
		DetectedAt:    time.Unix(1700000000, 0),
	}
	msg := buildMessage(t.Context(), ch).(*streamamqp.Message)
	if msg.ApplicationProperties["feed_url"] != "https://example.com/feed" {
		t.Fatalf("feed_url missing: %+v", msg.ApplicationProperties)
	}
	if msg.Properties == nil || msg.Properties.MessageID != "item-7" {
		t.Fatalf("MessageID not set: %+v", msg.Properties)
	}
}
```

(`buildMessage` returns `message.StreamMessage`; the concrete type is `*streamamqp.Message`. If the field for the message id differs in the installed version, use the one exposed by `streamamqp.Message.Properties`.)

- [ ] **Step 3: Run to verify they fail**

Run: `go test ./internal/sink/rabbitmqstream/ -v`
Expected: FAIL — undefined `New`/`buildMessage`.

- [ ] **Step 4: Implement the package**

`internal/sink/rabbitmqstream/rabbitmqstream.go`:

```go
// Package rabbitmqstream implements the sink.Publisher interface against a
// RabbitMQ Stream (native stream protocol, port 5552) via
// rabbitmq-stream-go-client. One Environment + one Producer per Publisher;
// Publish serialises one in-flight message and blocks on the broker
// confirmation so a returned nil means the message was confirmed.
package rabbitmqstream

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	streamamqp "github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/message"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

	"github.com/iambod/rss2msg/internal/model"
)

type Options struct {
	Name           string
	URIs           []string
	URL            string
	Stream         string
	Username       string
	Password       string
	Declare        bool
	MaxAge         time.Duration
	MaxLengthBytes int64
	TLS            *TLSOptions
}

type TLSOptions struct {
	CAFile             string
	CertFile           string
	KeyFile            string
	ServerName         string
	InsecureSkipVerify bool
}

type Publisher struct {
	name     string
	env      *stream.Environment
	producer *stream.Producer
	confirms <-chan []*stream.ConfirmationStatus

	mu sync.Mutex // one in-flight publish + confirmation at a time
}

func New(ctx context.Context, opts Options) (*Publisher, error) {
	if opts.Name == "" {
		return nil, fmt.Errorf("rabbitmq_stream sink: name is required")
	}
	if opts.Stream == "" {
		return nil, fmt.Errorf("rabbitmq_stream sink %q: stream is required", opts.Name)
	}
	if len(opts.URIs) == 0 && opts.URL == "" {
		return nil, fmt.Errorf("rabbitmq_stream sink %q: uris or url is required", opts.Name)
	}

	envOpts, err := buildEnvOptions(opts)
	if err != nil {
		return nil, fmt.Errorf("rabbitmq_stream sink %q: %w", opts.Name, err)
	}
	env, err := stream.NewEnvironment(envOpts)
	if err != nil {
		return nil, fmt.Errorf("rabbitmq_stream sink %q: environment: %w", opts.Name, err)
	}

	if opts.Declare {
		so := stream.NewStreamOptions()
		if opts.MaxAge > 0 {
			so = so.SetMaxAge(opts.MaxAge)
		}
		if opts.MaxLengthBytes > 0 {
			so = so.SetMaxLengthBytes(stream.ByteCapacity{}.B(opts.MaxLengthBytes))
		}
		if derr := env.DeclareStream(opts.Stream, so); derr != nil && !errors.Is(derr, stream.StreamAlreadyExists) {
			_ = env.Close()
			return nil, fmt.Errorf("rabbitmq_stream sink %q: declare stream %q: %w", opts.Name, opts.Stream, derr)
		}
	}

	producer, err := env.NewProducer(opts.Stream, stream.NewProducerOptions())
	if err != nil {
		_ = env.Close()
		return nil, fmt.Errorf("rabbitmq_stream sink %q: producer: %w", opts.Name, err)
	}

	return &Publisher{
		name:     opts.Name,
		env:      env,
		producer: producer,
		confirms: producer.NotifyPublishConfirmation(),
	}, nil
}

func buildEnvOptions(opts Options) (*stream.EnvironmentOptions, error) {
	eo := stream.NewEnvironmentOptions()
	if len(opts.URIs) > 0 {
		eo = eo.SetUris(opts.URIs)
	} else {
		eo = eo.SetUri(opts.URL)
	}
	if opts.Username != "" {
		eo = eo.SetUser(opts.Username)
	}
	if opts.Password != "" {
		eo = eo.SetPassword(opts.Password)
	}
	if opts.TLS != nil {
		tc, err := buildTLSConfig(*opts.TLS)
		if err != nil {
			return nil, fmt.Errorf("build TLS config: %w", err)
		}
		if opts.TLS.InsecureSkipVerify {
			log.Warn().Str("sink", opts.Name).Str("sink_driver", "rabbitmq_stream").
				Msg("rabbitmq_stream sink: TLS verification disabled (insecure_skip_verify=true)")
		}
		eo = eo.IsTLS(true).SetTLSConfig(tc)
	}
	return eo, nil
}

func buildMessage(ctx context.Context, change model.Change) message.StreamMessage {
	body, _ := json.Marshal(change)
	msg := streamamqp.NewMessage(body)

	props := map[string]any{
		"feed_url":       change.FeedURL,
		"kind":           string(change.Kind),
		"schema_version": int32(change.SchemaVersion),
	}
	if change.DLQFromSink != "" {
		props["dlq_from_sink"] = change.DLQFromSink
		props["dlq_error"] = change.DLQError
		props["dlq_attempts"] = int32(change.DLQAttempts)
	}
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	if tp, ok := carrier["traceparent"]; ok {
		props["traceparent"] = tp
	}
	if ts, ok := carrier["tracestate"]; ok && ts != "" {
		props["tracestate"] = ts
	}
	msg.ApplicationProperties = props
	msg.Properties = &streamamqp.MessageProperties{
		MessageID:   change.ItemID,
		ContentType: "application/json",
	}
	return msg
}

func buildTLSConfig(opts TLSOptions) (*tls.Config, error) {
	tc := &tls.Config{InsecureSkipVerify: opts.InsecureSkipVerify} //nolint:gosec // opt-in, logged at warn
	if opts.ServerName != "" {
		tc.ServerName = opts.ServerName
	}
	if opts.CAFile != "" {
		pem, err := os.ReadFile(opts.CAFile)
		if err != nil {
			return nil, fmt.Errorf("ca_file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("ca_file %q: no PEM certificates parsed", opts.CAFile)
		}
		tc.RootCAs = pool
	}
	if opts.CertFile != "" || opts.KeyFile != "" {
		if opts.CertFile == "" || opts.KeyFile == "" {
			return nil, fmt.Errorf("cert_file and key_file must both be set or both empty")
		}
		cert, err := tls.LoadX509KeyPair(opts.CertFile, opts.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("client cert: %w", err)
		}
		tc.Certificates = []tls.Certificate{cert}
	}
	return tc, nil
}

func (p *Publisher) Name() string { return p.name }

func (p *Publisher) Publish(ctx context.Context, change model.Change) error {
	msg := buildMessage(ctx, change)
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.producer.Send(msg); err != nil {
		return fmt.Errorf("rabbitmq_stream sink %q: send: %w", p.name, err)
	}
	// One in-flight message at a time (mutex-guarded): the next confirmation
	// batch is ours.
	select {
	case batch := <-p.confirms:
		for _, st := range batch {
			if !st.IsConfirmed() {
				return fmt.Errorf("rabbitmq_stream sink %q: publish not confirmed: %w", p.name, st.GetError())
			}
		}
		return nil
	case <-ctx.Done():
		return fmt.Errorf("rabbitmq_stream sink %q: confirmation wait: %w", p.name, ctx.Err())
	}
}

func (p *Publisher) Close() error {
	if p.producer != nil {
		_ = p.producer.Close()
	}
	if p.env != nil {
		return p.env.Close()
	}
	return nil
}
```

Add `"errors"` to the import block (used by the `DeclareStream` idempotency check). Verify the exact names against the installed version: `stream.StreamAlreadyExists` (sentinel), `ByteCapacity{}.B(int64)`, `ConfirmationStatus.GetError()`, and `streamamqp.MessageProperties.MessageID`/`ContentType`. If any differ, adjust the reference — the structure of the code stays the same.

- [ ] **Step 5: Run unit tests + build**

Run: `go test ./internal/sink/rabbitmqstream/ -v && task build`
Expected: PASS, module compiles end-to-end.

- [ ] **Step 6: Commit**

```bash
git add internal/sink/rabbitmqstream/rabbitmqstream.go internal/sink/rabbitmqstream/rabbitmqstream_unit_test.go go.mod go.sum
git commit -m "feat(sink): add rabbitmq_stream sink (native stream protocol)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: Integration test, docs, examples, CI shard

**Files:**
- Create: `internal/sink/rabbitmqstream/rabbitmqstream_test.go` (`//go:build integration`)
- Create: `docs/how-to/sinks/rabbitmq-stream.md`
- Modify: sink index, `README.md`, both example YAMLs
- Modify: CI integration matrix (assign the new package to the existing **rabbitmq shard**)

**Interfaces:**
- Consumes: `New`, `Publish`, `Close` from Task 2.

- [ ] **Step 1: Write the integration test**

`internal/sink/rabbitmqstream/rabbitmqstream_test.go`: start RabbitMQ with the `rabbitmq_stream` plugin enabled and port 5552 exposed. Use a testcontainers request based on `rabbitmq:4-management`, injecting an `enabled_plugins` file via `Files` and exposing `5552/tcp`:

```go
//go:build integration

package rabbitmqstream

// Container request:
//   Image: "rabbitmq:4-management"
//   ExposedPorts: []string{"5552/tcp"}
//   Files: write "[rabbitmq_management,rabbitmq_stream]." to
//          /etc/rabbitmq/enabled_plugins (ContainerFilePath)
//   Env: {"RABBITMQ_DEFAULT_USER":"guest","RABBITMQ_DEFAULT_PASS":"guest"}
//   WaitingFor: wait.ForListeningPort("5552/tcp")
// Then:
//   1. host:port from the mapped 5552 port
//   2. p, _ := New(ctx, Options{Name:"t", URIs:[]string{uri}, Stream:"itest", Declare:true})
//   3. err := p.Publish(ctx, model.Change{FeedURL:"u", Kind:model.ChangeKindNew, ItemID:"1"})
//      assert err == nil (confirmed)
//   4. Consume via env.NewConsumer("itest", handler, stream.NewConsumerOptions().
//      SetOffset(stream.OffsetSpecification{}.First())) and assert the body
//      round-trips and feed_url app property == "u".
```

- [ ] **Step 2: Run the integration test (Docker required)**

Run: `go test -tags=integration ./internal/sink/rabbitmqstream/ -run TestRabbitMQStream -v`
Expected: PASS.

- [ ] **Step 3: Assign the package to the rabbitmq CI shard**

Find the integration test matrix in `.github/workflows/` (the package-sharded matrix that fixed runner disk exhaustion). Add `./internal/sink/rabbitmqstream/...` to the **same shard entry** that already runs `./internal/sink/amqp091/...` (the RabbitMQ-image shard), so both reuse the one pulled RabbitMQ image rather than spinning a new runner.

- [ ] **Step 4: Write the how-to doc**

Create `docs/how-to/sinks/rabbitmq-stream.md` with standard frontmatter (`updated: 2026-06-20`) and a `## Related` footer. Document: the `rabbitmq_stream` config block, that this uses the native stream protocol on port 5552 (not AMQP), `uris` vs `url`, `declare` + `max_age`/`max_length_bytes` retention, auth precedence, TLS, and a note that publisher-side dedup is not yet supported.

- [ ] **Step 5: Update sink index, README, and both example YAMLs**

Add `rabbitmq_stream` to `docs/how-to/choose-a-sink.md` and `README.md`. Append the identical block to BOTH example YAMLs:

```yaml
  # - name: stream-main
  #   driver: rabbitmq_stream
  #   rabbitmq_stream:
  #     uris: ["rabbitmq-stream://guest:guest@rabbit-1:5552/%2f"]  # or a single url:
  #     stream: feed.changes                          # target stream (required)
  #     declare: true                                 # create the stream if absent
  #     max_age: 168h                                 # optional retention (declare only)
  #     max_length_bytes: 5368709120                  # optional retention (declare only)
  #     # username: ${RMQ_USER}                       # optional; else URI userinfo
  #     # password: ${RMQ_PASS}
  #     tls:                                          # use rabbitmq-stream+tls / IsTLS; custom CA / mTLS
  #       ca_file: /etc/ssl/certs/rabbit-ca.pem
  #   dead_letter: dlq-main
```

- [ ] **Step 6: Run the full gate**

Run: `task test && task vet && task lint && bash scripts/check-doc-links.sh`
Expected: all PASS; link checker prints `OK: all relative doc links resolve`.

- [ ] **Step 7: Commit**

```bash
git add internal/sink/rabbitmqstream/rabbitmqstream_test.go docs/how-to/sinks/rabbitmq-stream.md docs/how-to/choose-a-sink.md README.md examples/config.example.yaml internal/config/example.yaml .github/workflows
git status
git commit -m "test(sink): rabbitmq_stream integration test; docs, examples, CI shard

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

## Self-Review

- **Spec coverage:** rabbitmq_stream driver, config (uris/url/stream/username/password/declare/max_age/max_length_bytes/tls), synchronous confirmed publish, deferred dedup (documented), metadata + trace keys, validation, wiring, unit + integration tests, docs, both example YAMLs, CI shard assignment — all covered across Tasks 1-3.
- **Placeholders:** the integration test (Task 3 Step 1) and CI shard edit are described against existing files rather than reproduced verbatim, because both depend on repo-specific helpers/workflow YAML the implementer must mirror. All production code is complete. The code flags three version-sensitive identifiers (`stream.StreamAlreadyExists`, `ByteCapacity{}.B`, `ConfirmationStatus.GetError`, `MessageProperties` fields) to verify against the pinned version — grounded in the client README, not invented.
- **Type consistency:** `RabbitMQStreamSinkConfig`/field `RabbitMQStream`/key `rabbitmq_stream`/alias `sinkrabbitmqstream`/`Options`/`TLSOptions`/`buildMessage`/`buildEnvOptions` consistent across tasks. `New(ctx, Options)` matches the wire `case`.
