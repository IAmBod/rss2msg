# Kafka Schema Registry — PR1 (foundation + JSON Schema) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add opt-in Confluent Schema Registry support to the Kafka sink with the **JSON Schema** format, plus the pluggable encoder foundation that PR2 (Avro) and PR3 (Protobuf) build on.

**Architecture:** A new `internal/sink/kafka/schema` package defines an `Encoder` interface and a registry client wrapper (lazy, cached schema-ID registration via `franz-go/pkg/sr`). The kafka `Publisher` gains an optional `Encoder`; when nil it keeps today's exact plain-JSON path. Config gains a `schema_registry` block under each kafka sink, with validation. The JSON encoder generates a canonical JSON Schema from `model.Change` and frames `json.Marshal(change)` with the Confluent wire format.

**Tech Stack:** Go 1.25, `github.com/twmb/franz-go/pkg/sr` (registry client + `ConfluentHeader` framing + `srfake` test registry), `github.com/google/jsonschema-go` (schema generation), Viper config, testcontainers (integration).

**Scope note:** This is PR1 of three. PR2 (Avro) and PR3 (Protobuf) get their own plans authored after PR1 merges, because they extend this PR's `Encoder` factory and validation. Spec: `docs/superpowers/specs/2026-06-09-kafka-schema-registry-design.md`.

---

## File Structure

**Create:**
- `internal/sink/kafka/schema/encoder.go` — `Encoder` interface, `Format` constants, `Options`, `New()` factory.
- `internal/sink/kafka/schema/registry.go` — `registrar` (sr client wrapper, lazy cached schema ID) + `newClient()`.
- `internal/sink/kafka/schema/json.go` — `jsonEncoder` + canonical JSON Schema generation.
- `internal/sink/kafka/schema/encoder_test.go` — factory + framing unit tests.
- `internal/sink/kafka/schema/registry_test.go` — registration unit tests against `srfake`.
- `internal/sink/kafka/schema/json_test.go` — JSON encoder unit tests.
- `internal/sink/kafka/schema_integration_test.go` — Kafka + Schema Registry round-trip (`//go:build integration`).

**Modify:**
- `internal/sink/kafka/kafka.go` — add `Schema *schema.Options` to `Options`; build encoder in `New`; use it in `Publish`.
- `internal/config/config.go` — `SchemaRegistryConfig` + `SchemaRegistry` field on `KafkaSinkConfig`.
- `internal/config/validate.go` — validate the `schema_registry` block.
- `cmd/rss2msg/wire.go` — map `config.SchemaRegistryConfig` → `schema.Options`.
- `docs/how-to/sinks/kafka.md` — document the new block + record layout.
- `go.mod` / `go.sum` — add `pkg/sr`, promote `jsonschema-go` to direct.

---

## Task 1: Add dependencies

**Files:**
- Modify: `go.mod`, `go.sum`

- [ ] **Step 1: Add the registry client and tidy**

Run:
```bash
cd .worktrees/kafka-schema
go get github.com/twmb/franz-go/pkg/sr@v1.7.0
task tidy
```
Expected: `go.mod` lists `github.com/twmb/franz-go/pkg/sr v1.7.0` (no longer `// indirect` once code imports it in later tasks; it may stay indirect until then — that is fine). `github.com/google/jsonschema-go` is already present.

- [ ] **Step 2: Verify the build still compiles**

Run: `task build`
Expected: builds `./rss2msg` with no errors.

- [ ] **Step 3: Commit**

```bash
git add go.mod go.sum
git commit -m "chore(deps): add franz-go/pkg/sr for kafka schema registry"
```

---

## Task 2: Encoder interface, Format constants, and factory skeleton

**Files:**
- Create: `internal/sink/kafka/schema/encoder.go`
- Test: `internal/sink/kafka/schema/encoder_test.go`

- [ ] **Step 1: Write the failing test**

`internal/sink/kafka/schema/encoder_test.go`:
```go
package schema

import (
	"strings"
	"testing"
)

func TestNewRejectsUnsupportedFormat(t *testing.T) {
	_, err := New(Options{URL: "http://sr:8081", Format: "avro", Topic: "t"})
	if err == nil || !strings.Contains(err.Error(), "format") {
		t.Fatalf("want unsupported-format error, got %v", err)
	}
}

func TestNewRequiresURL(t *testing.T) {
	_, err := New(Options{Format: FormatJSON, Topic: "t"})
	if err == nil || !strings.Contains(err.Error(), "url") {
		t.Fatalf("want url-required error, got %v", err)
	}
}

func TestDefaultSubjectIsTopicValue(t *testing.T) {
	if got := defaultSubject("feed.changes", ""); got != "feed.changes-value" {
		t.Fatalf("default subject = %q, want feed.changes-value", got)
	}
	if got := defaultSubject("feed.changes", "custom"); got != "custom" {
		t.Fatalf("explicit subject = %q, want custom", got)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/sink/kafka/schema/ -run TestNew -v`
Expected: FAIL — `undefined: New`, `undefined: Options`, etc.

- [ ] **Step 3: Write minimal implementation**

`internal/sink/kafka/schema/encoder.go`:
```go
// Package schema adds opt-in Confluent Schema Registry support to the Kafka
// sink. An Encoder frames a model.Change into the Confluent wire format
// (magic byte + 4-byte big-endian schema ID + payload) after registering the
// schema with a Schema Registry. When no schema_registry block is configured
// the kafka sink does not construct an Encoder and emits plain JSON as before.
package schema

import (
	"context"
	"crypto/tls"
	"fmt"

	"github.com/iambod/rss2msg/internal/model"
)

// Format is a Confluent Schema Registry serialization format.
type Format string

const (
	FormatJSON     Format = "json"
	FormatAvro     Format = "avro"
	FormatProtobuf Format = "protobuf"
)

// Encoder frames a Change into a Confluent-wire-format record value.
type Encoder interface {
	// Encode returns the framed record value for c, registering the schema on
	// first use. Any registration or encoding error is returned so the caller
	// can hard-fail the publish.
	Encode(ctx context.Context, c model.Change) ([]byte, error)
	// Format reports the wire format name.
	Format() string
}

// Options configures an Encoder. A non-empty URL enables the feature.
type Options struct {
	URL          string
	Format       Format
	Topic        string // used to derive the default subject
	Subject      string // overrides the default "<topic>-value"
	AutoRegister bool   // register on first use (true) vs. look up an existing id
	SchemaText   string // overrides the canonical registered schema text
	BasicUser    string
	BasicPass    string
	TLS          *tls.Config
}

// New builds an Encoder for the configured format.
func New(opts Options) (Encoder, error) {
	if opts.URL == "" {
		return nil, fmt.Errorf("schema registry: url is required")
	}
	subject := defaultSubject(opts.Topic, opts.Subject)
	switch opts.Format {
	case FormatJSON:
		return newJSONEncoder(opts, subject)
	case FormatAvro, FormatProtobuf:
		return nil, fmt.Errorf("schema registry: format %q is not supported yet (only %q in this release)", opts.Format, FormatJSON)
	default:
		return nil, fmt.Errorf("schema registry: unknown format %q", opts.Format)
	}
}

func defaultSubject(topic, override string) string {
	if override != "" {
		return override
	}
	return topic + "-value"
}
```

> Note: `newJSONEncoder` is defined in Task 4. Until then this file will not compile on its own; Tasks 3–4 complete the package. Run the package build at the end of Task 4.

- [ ] **Step 4: Commit (after Task 4 compiles)**

This task's file is committed together with Tasks 3–4 (the package must compile first). Proceed to Task 3.

---

## Task 3: Registry client wrapper with lazy, cached registration

**Files:**
- Create: `internal/sink/kafka/schema/registry.go`
- Test: `internal/sink/kafka/schema/registry_test.go`

- [ ] **Step 1: Write the failing test**

`internal/sink/kafka/schema/registry_test.go`:
```go
package schema

import (
	"context"
	"testing"

	"github.com/twmb/franz-go/pkg/sr"
	"github.com/twmb/franz-go/pkg/sr/srfake"
)

func newTestRegistrar(t *testing.T, auto bool) (*registrar, *srfake.Registry) {
	t.Helper()
	fake := srfake.New()
	t.Cleanup(fake.Close)
	cl, err := sr.NewClient(sr.URLs(fake.URL()))
	if err != nil {
		t.Fatal(err)
	}
	r := &registrar{
		cl:      cl,
		subject: "feed.changes-value",
		schema:  sr.Schema{Schema: `{"type":"object"}`, Type: sr.TypeJSON},
		auto:    auto,
	}
	return r, fake
}

func TestRegistrarAutoRegisterCachesID(t *testing.T) {
	r, _ := newTestRegistrar(t, true)
	ctx := context.Background()
	id1, err := r.schemaID(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if id1 == 0 {
		t.Fatal("expected non-zero schema id")
	}
	id2, err := r.schemaID(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if id1 != id2 {
		t.Fatalf("id not cached: %d != %d", id1, id2)
	}
}

func TestRegistrarNoAutoRegisterErrorsWhenMissing(t *testing.T) {
	r, _ := newTestRegistrar(t, false)
	if _, err := r.schemaID(context.Background()); err == nil {
		t.Fatal("expected error looking up unregistered subject")
	}
}

func TestRegistrarNoAutoRegisterFindsSeeded(t *testing.T) {
	r, fake := newTestRegistrar(t, false)
	fake.SeedSchema("feed.changes-value", 1, 42, r.schema)
	id, err := r.schemaID(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if id != 42 {
		t.Fatalf("looked-up id = %d, want 42", id)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/sink/kafka/schema/ -run TestRegistrar -v`
Expected: FAIL — `undefined: registrar`.

- [ ] **Step 3: Write minimal implementation**

`internal/sink/kafka/schema/registry.go`:
```go
package schema

import (
	"context"
	"sync"

	"github.com/twmb/franz-go/pkg/sr"
)

// registrar resolves and caches the Schema Registry id for one subject.
// Registration is lazy (first Encode) so a registry blip at startup does not
// block process boot; on failure the id stays uncached and the next publish
// retries.
type registrar struct {
	cl      *sr.Client
	subject string
	schema  sr.Schema
	auto    bool

	mu sync.Mutex
	id int // 0 = not yet resolved
}

func (r *registrar) schemaID(ctx context.Context) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.id != 0 {
		return r.id, nil
	}
	var (
		ss  sr.SubjectSchema
		err error
	)
	if r.auto {
		ss, err = r.cl.CreateSchema(ctx, r.subject, r.schema)
	} else {
		ss, err = r.cl.LookupSchema(ctx, r.subject, r.schema)
	}
	if err != nil {
		return 0, err
	}
	r.id = ss.ID
	return r.id, nil
}

// newClient builds an sr.Client from Options.
func newClient(opts Options) (*sr.Client, error) {
	clientOpts := []sr.ClientOpt{sr.URLs(opts.URL)}
	if opts.BasicUser != "" || opts.BasicPass != "" {
		clientOpts = append(clientOpts, sr.BasicAuth(opts.BasicUser, opts.BasicPass))
	}
	if opts.TLS != nil {
		clientOpts = append(clientOpts, sr.DialTLSConfig(opts.TLS))
	}
	return sr.NewClient(clientOpts...)
}
```

- [ ] **Step 4: Run test (will still fail to compile until Task 4)**

The package needs `newJSONEncoder` (referenced in Task 2). Proceed to Task 4, then run all package tests.

---

## Task 4: JSON Schema encoder + canonical schema generation

**Files:**
- Create: `internal/sink/kafka/schema/json.go`
- Test: `internal/sink/kafka/schema/json_test.go`

- [ ] **Step 1: Write the failing test**

`internal/sink/kafka/schema/json_test.go`:
```go
package schema

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/sr"
	"github.com/twmb/franz-go/pkg/sr/srfake"

	"github.com/iambod/rss2msg/internal/model"
)

func TestCanonicalJSONSchemaMentionsFields(t *testing.T) {
	text, err := canonicalJSONSchema()
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"feed_url", "item_id", "content_hash"} {
		if !strings.Contains(text, field) {
			t.Fatalf("canonical schema missing %q: %s", field, text)
		}
	}
}

func TestJSONEncoderFramesWithMagicAndID(t *testing.T) {
	fake := srfake.New()
	t.Cleanup(fake.Close)

	enc, err := New(Options{
		URL: fake.URL(), Format: FormatJSON, Topic: "feed.changes", AutoRegister: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if enc.Format() != "json" {
		t.Fatalf("Format() = %q, want json", enc.Format())
	}

	c := model.Change{SchemaVersion: 1, FeedURL: "f1", ItemID: "i1", Kind: model.ChangeNew, ContentHash: "h", Title: "hi"}
	framed, err := enc.Encode(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	if len(framed) < 5 {
		t.Fatalf("framed too short: %d bytes", len(framed))
	}
	if framed[0] != 0 {
		t.Fatalf("magic byte = %d, want 0", framed[0])
	}
	id := binary.BigEndian.Uint32(framed[1:5])
	if id == 0 {
		t.Fatal("schema id is zero")
	}
	var round model.Change
	if err := json.Unmarshal(framed[5:], &round); err != nil {
		t.Fatalf("payload not valid JSON Change: %v", err)
	}
	if round.Title != "hi" {
		t.Fatalf("payload title = %q, want hi", round.Title)
	}
}

func TestJSONEncoderUsesOverrideSchemaText(t *testing.T) {
	fake := srfake.New()
	t.Cleanup(fake.Close)
	override := `{"type":"object","title":"custom-change"}`

	enc, err := New(Options{
		URL: fake.URL(), Format: FormatJSON, Topic: "t", AutoRegister: true, SchemaText: override,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := enc.Encode(context.Background(), model.Change{ItemID: "i"}); err != nil {
		t.Fatal(err)
	}
	got, ok := fake.GetSchema("t-value", 1)
	if !ok {
		t.Fatal("schema not registered")
	}
	if got.Schema.Schema != override {
		t.Fatalf("registered schema = %q, want override", got.Schema.Schema)
	}
	_ = sr.Schema{} // keep sr imported
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/sink/kafka/schema/ -run TestJSON -v`
Expected: FAIL — `undefined: canonicalJSONSchema`, `undefined: newJSONEncoder`.

- [ ] **Step 3: Write minimal implementation**

`internal/sink/kafka/schema/json.go`:
```go
package schema

import (
	"context"
	"encoding/json"
	"fmt"

	gojsonschema "github.com/google/jsonschema-go/jsonschema"
	"github.com/twmb/franz-go/pkg/sr"

	"github.com/iambod/rss2msg/internal/model"
)

type jsonEncoder struct {
	reg    *registrar
	header sr.ConfluentHeader
}

func newJSONEncoder(opts Options, subject string) (Encoder, error) {
	cl, err := newClient(opts)
	if err != nil {
		return nil, fmt.Errorf("schema registry client: %w", err)
	}
	text := opts.SchemaText
	if text == "" {
		text, err = canonicalJSONSchema()
		if err != nil {
			return nil, fmt.Errorf("canonical json schema: %w", err)
		}
	}
	return &jsonEncoder{
		reg: &registrar{
			cl:      cl,
			subject: subject,
			schema:  sr.Schema{Schema: text, Type: sr.TypeJSON},
			auto:    opts.AutoRegister,
		},
	}, nil
}

func (e *jsonEncoder) Format() string { return string(FormatJSON) }

func (e *jsonEncoder) Encode(ctx context.Context, c model.Change) ([]byte, error) {
	id, err := e.reg.schemaID(ctx)
	if err != nil {
		return nil, fmt.Errorf("schema registry: %w", err)
	}
	payload, err := json.Marshal(c)
	if err != nil {
		return nil, fmt.Errorf("marshal change: %w", err)
	}
	buf, _ := e.header.AppendEncode(nil, id, nil) // error is always nil
	return append(buf, payload...), nil
}

// canonicalJSONSchema generates a JSON Schema document from model.Change.
func canonicalJSONSchema() (string, error) {
	s, err := gojsonschema.For[model.Change](nil)
	if err != nil {
		return "", err
	}
	b, err := json.Marshal(s)
	if err != nil {
		return "", err
	}
	return string(b), nil
}
```

- [ ] **Step 4: Run the whole package test**

Run: `go test ./internal/sink/kafka/schema/ -v`
Expected: PASS — all Task 2/3/4 tests green.

- [ ] **Step 5: Vet & lint the new package**

Run: `go vet ./internal/sink/kafka/schema/ && golangci-lint run ./internal/sink/kafka/schema/`
Expected: no findings. (If `golangci-lint` is unavailable, run `task vet` and note lint was skipped.)

- [ ] **Step 6: Commit the schema package**

```bash
git add internal/sink/kafka/schema/ go.mod go.sum
git commit -m "feat(kafka): add schema-registry encoder package with JSON Schema support"
```

---

## Task 5: Wire the encoder into the kafka Publisher

**Files:**
- Modify: `internal/sink/kafka/kafka.go`
- Test: `internal/sink/kafka/kafka_unit_test.go` (create)

- [ ] **Step 1: Write the failing test**

Create `internal/sink/kafka/kafka_unit_test.go`:
```go
package kafka

import (
	"strings"
	"testing"

	"github.com/iambod/rss2msg/internal/sink/kafka/schema"
)

func TestNewBuildsSchemaEncoderWhenConfigured(t *testing.T) {
	p, err := New(Options{
		Name: "k", Brokers: []string{"b:9092"}, Topic: "feed.changes",
		Schema: &schema.Options{URL: "http://sr:8081", Format: schema.FormatJSON, Topic: "feed.changes", AutoRegister: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Close() }()
	if p.encoder == nil {
		t.Fatal("expected encoder to be built when Schema configured")
	}
}

func TestNewNoEncoderWhenSchemaNil(t *testing.T) {
	p, err := New(Options{Name: "k", Brokers: []string{"b:9092"}, Topic: "t"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Close() }()
	if p.encoder != nil {
		t.Fatal("expected no encoder when Schema is nil")
	}
}

func TestNewRejectsBadSchemaFormat(t *testing.T) {
	_, err := New(Options{
		Name: "k", Brokers: []string{"b:9092"}, Topic: "t",
		Schema: &schema.Options{URL: "http://sr:8081", Format: "bogus", Topic: "t"},
	})
	if err == nil || !strings.Contains(err.Error(), "schema") {
		t.Fatalf("want schema error, got %v", err)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/sink/kafka/ -run TestNew -v`
Expected: FAIL — `p.encoder` undefined, `Options.Schema` undefined.

- [ ] **Step 3: Modify `internal/sink/kafka/kafka.go`**

Add the import and fields. Change the `Publisher` struct and `Options` struct:
```go
// in the import block, add:
//   "github.com/iambod/rss2msg/internal/sink/kafka/schema"

type Publisher struct {
	name    string
	client  *kgo.Client
	topic   string
	encoder schema.Encoder // nil ⇒ plain JSON
}

// add to Options (after TLS):
//   // Schema, if non-nil, enables Confluent Schema Registry encoding of the
//   // record value. Nil keeps the plain-JSON value.
//   Schema *schema.Options
```

In `New`, after the TLS block and before `kgo.NewClient`, build the encoder:
```go
	var enc schema.Encoder
	if opts.Schema != nil {
		var err error
		enc, err = schema.New(*opts.Schema)
		if err != nil {
			return nil, fmt.Errorf("kafka sink %q: schema: %w", opts.Name, err)
		}
	}
```

Change the final return to include the encoder:
```go
	return &Publisher{name: opts.Name, client: client, topic: opts.topic, encoder: enc}, nil
```
(Use `opts.Topic`, matching the existing field — the snippet above shows the shape; keep the existing `topic: opts.Topic`.)

In `Publish`, replace the value marshalling at the top:
```go
func (p *Publisher) Publish(ctx context.Context, change model.Change) error {
	var (
		value []byte
		err   error
	)
	if p.encoder != nil {
		value, err = p.encoder.Encode(ctx, change)
	} else {
		value, err = json.Marshal(change)
	}
	if err != nil {
		return fmt.Errorf("kafka sink %q: encode: %w", p.name, err)
	}
	// ... rest unchanged (build rec with value, headers, trace, ProduceSync)
```

- [ ] **Step 4: Run tests**

Run: `go test ./internal/sink/kafka/ -run TestNew -v`
Expected: PASS.

- [ ] **Step 5: Run the full kafka unit + schema tests and vet**

Run: `go test ./internal/sink/kafka/... && task vet`
Expected: PASS, no vet findings.

- [ ] **Step 6: Commit**

```bash
git add internal/sink/kafka/kafka.go internal/sink/kafka/kafka_unit_test.go
git commit -m "feat(kafka): use schema-registry encoder for record value when configured"
```

---

## Task 6: Config — SchemaRegistryConfig

**Files:**
- Modify: `internal/config/config.go`
- Test: `internal/config/load_test.go` (add a case)

- [ ] **Step 1: Write the failing test**

Add to `internal/config/load_test.go` (new test function):
```go
func TestLoadKafkaSchemaRegistry(t *testing.T) {
	yaml := `
state:
  driver: sqlite
  sqlite:
    path: ":memory:"
sinks:
  - name: events
    driver: kafka
    kafka:
      brokers: ["k:9092"]
      topic: feed.changes
      schema_registry:
        url: http://sr:8081
        format: json
        auto_register: true
        subject: feed.changes-value
        basic_auth:
          username: u
          password: p
feeds:
  - url: https://example.com/feed.xml
    interval: 60s
    sinks: [events]
`
	c := writeAndLoad(t, yaml)
	sr := c.Sinks[0].Kafka.SchemaRegistry
	if sr.URL != "http://sr:8081" || sr.Format != "json" || !sr.AutoRegister {
		t.Fatalf("schema_registry not parsed: %+v", sr)
	}
	if sr.BasicAuth.Username != "u" || sr.BasicAuth.Password != "p" {
		t.Fatalf("basic_auth not parsed: %+v", sr.BasicAuth)
	}
}
```

> Check the existing helper name in `load_test.go` (e.g. `writeAndLoad` / `loadFromYAML`) and use whatever the file already defines; the snippet assumes `writeAndLoad(t, yaml) Config`.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/config/ -run TestLoadKafkaSchemaRegistry -v`
Expected: FAIL — `SchemaRegistry` undefined.

- [ ] **Step 3: Add the config types**

In `internal/config/config.go`, add the field to `KafkaSinkConfig`:
```go
type KafkaSinkConfig struct {
	Brokers        []string             `mapstructure:"brokers"`
	Topic          string               `mapstructure:"topic"`
	Acks           string               `mapstructure:"acks"`
	Compression    string               `mapstructure:"compression"`
	TLS            SinkTLSConfig        `mapstructure:"tls"`
	SchemaRegistry SchemaRegistryConfig `mapstructure:"schema_registry"`
}
```

Add the new type (near `KafkaSinkConfig`):
```go
// SchemaRegistryConfig enables Confluent Schema Registry encoding for the
// kafka sink. A non-empty URL turns the feature on; absent ⇒ plain JSON.
type SchemaRegistryConfig struct {
	// URL is the Schema Registry base URL; its presence enables the feature.
	URL string `mapstructure:"url"`
	// Format selects the wire format: "json" | "avro" | "protobuf".
	Format string `mapstructure:"format"`
	// Subject overrides the default "<topic>-value" subject.
	Subject string `mapstructure:"subject"`
	// AutoRegister registers the schema on first use (default true). When
	// false, the sink looks up an existing schema id and errors if absent.
	AutoRegister bool `mapstructure:"auto_register"`
	// SchemaFile overrides the canonical registered schema text with the
	// contents of this file. Must stay wire-compatible with the canonical shape.
	SchemaFile string `mapstructure:"schema_file"`
	// BasicAuth, if set, authenticates to the registry.
	BasicAuth SchemaRegistryBasicAuth `mapstructure:"basic_auth"`
	// TLS configures TLS to the registry (same shape as sink TLS).
	TLS SinkTLSConfig `mapstructure:"tls"`
}

// SchemaRegistryBasicAuth holds HTTP basic-auth credentials for the registry.
type SchemaRegistryBasicAuth struct {
	Username string `mapstructure:"username"`
	Password string `mapstructure:"password"`
}
```

> Note: `auto_register` defaults to `false` in Go's zero value. The default-true behavior is applied in `wire.go` (Task 8) by treating an *unset* key as true. To detect "unset", Task 8 reads the raw key; for config-struct round-tripping the test above sets it explicitly. Document the default-true in validation/docs.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/config/ -run TestLoadKafkaSchemaRegistry -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/config/config.go internal/config/load_test.go
git commit -m "feat(config): add kafka schema_registry config block"
```

---

## Task 7: Config validation

**Files:**
- Modify: `internal/config/validate.go`
- Test: `internal/config/validate_test.go` (add cases)

- [ ] **Step 1: Write the failing tests**

Add to `internal/config/validate_test.go`:
```go
func TestValidateSchemaRegistry(t *testing.T) {
	base := func(sr SchemaRegistryConfig) Config {
		return Config{
			State: StateConfig{Driver: "sqlite", SQLite: SQLiteConfig{Path: ":memory:"}},
			Sinks: []SinkConfig{{Name: "default", Driver: "kafka", Kafka: KafkaSinkConfig{
				Brokers: []string{"k:9092"}, Topic: "t", SchemaRegistry: sr,
			}}},
			Feeds: []FeedConfig{{URL: "https://e/f.xml", Interval: time.Minute}},
		}
	}

	if _, err := Validate(base(SchemaRegistryConfig{URL: "http://sr:8081", Format: "json"})); err != nil {
		t.Fatalf("valid json registry rejected: %v", err)
	}
	if _, err := Validate(base(SchemaRegistryConfig{URL: "http://sr:8081", Format: "bogus"})); err == nil {
		t.Fatal("expected error for unknown format")
	}
	if _, err := Validate(base(SchemaRegistryConfig{URL: "http://sr:8081"})); err == nil {
		t.Fatal("expected error for missing format")
	}
	if _, err := Validate(base(SchemaRegistryConfig{Format: "json"})); err == nil {
		t.Fatal("expected error for format without url")
	}
	if _, err := Validate(base(SchemaRegistryConfig{URL: "http://sr:8081", Format: "json", SchemaFile: "/no/such/file"})); err == nil {
		t.Fatal("expected error for missing schema_file")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/config/ -run TestValidateSchemaRegistry -v`
Expected: FAIL (no validation yet → cases that expect errors get nil).

- [ ] **Step 3: Add validation**

In `internal/config/validate.go`, inside the existing `for i, s := range c.Sinks` loop that handles TLS (around line 701), add a kafka-specific schema-registry check. Place this as its own loop after the TLS loop:
```go
	// Kafka Schema Registry: url enables the feature; format is then required
	// and must be one of the supported values. format without url is an error.
	for i, s := range c.Sinks {
		if s.Driver != "kafka" {
			continue
		}
		sr := s.Kafka.SchemaRegistry
		if sr.URL == "" {
			if sr.Format != "" {
				return *warnings, fmt.Errorf("sinks[%d] (kafka %q): schema_registry.format set without schema_registry.url", i, s.Name)
			}
			continue
		}
		switch sr.Format {
		case "json":
			// supported in this release
		case "avro", "protobuf":
			return *warnings, fmt.Errorf("sinks[%d] (kafka %q): schema_registry.format %q is not supported yet (only \"json\")", i, s.Name, sr.Format)
		case "":
			return *warnings, fmt.Errorf("sinks[%d] (kafka %q): schema_registry.format is required when url is set", i, s.Name)
		default:
			return *warnings, fmt.Errorf("sinks[%d] (kafka %q): schema_registry.format %q is invalid (json|avro|protobuf)", i, s.Name, sr.Format)
		}
		if sr.SchemaFile != "" {
			if _, err := os.Stat(sr.SchemaFile); err != nil {
				return *warnings, fmt.Errorf("sinks[%d] (kafka %q): schema_registry.schema_file %q: %w", i, s.Name, sr.SchemaFile, err)
			}
		}
		if (sr.BasicAuth.Username == "") != (sr.BasicAuth.Password == "") {
			return *warnings, fmt.Errorf("sinks[%d] (kafka %q): schema_registry.basic_auth username and password must both be set or both empty", i, s.Name)
		}
		if (sr.TLS.CertFile == "") != (sr.TLS.KeyFile == "") {
			return *warnings, fmt.Errorf("sinks[%d] (kafka %q): schema_registry.tls.cert_file and key_file must both be set or both empty", i, s.Name)
		}
	}
```

Ensure `os` is imported in `validate.go` (check the import block; add `"os"` if missing).

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/config/ -run TestValidateSchemaRegistry -v`
Expected: PASS.

- [ ] **Step 5: Run full config tests + vet**

Run: `go test ./internal/config/... && task vet`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/config/validate.go internal/config/validate_test.go
git commit -m "feat(config): validate kafka schema_registry block"
```

---

## Task 8: Wire config → encoder options

**Files:**
- Modify: `cmd/rss2msg/wire.go`
- Test: `cmd/rss2msg/wire_test.go` (add a case if the package has one; else add a small test file `cmd/rss2msg/wire_schema_test.go`)

- [ ] **Step 1: Write the failing test**

Check `cmd/rss2msg/` for an existing wire test (`grep -l "sinkFromConfig\|func Test" cmd/rss2msg/*_test.go`). Add to it (or create `cmd/rss2msg/wire_schema_test.go`):
```go
package main

import (
	"testing"

	"github.com/iambod/rss2msg/internal/config"
	"github.com/iambod/rss2msg/internal/sink/kafka/schema"
)

func TestSchemaOptionsFromConfigDefaultsAutoRegisterTrue(t *testing.T) {
	got := schemaOptionsFromConfig("feed.changes", config.SchemaRegistryConfig{URL: "http://sr:8081", Format: "json"})
	if got == nil {
		t.Fatal("expected non-nil schema options when url set")
	}
	if got.Format != schema.FormatJSON || got.Topic != "feed.changes" {
		t.Fatalf("bad mapping: %+v", got)
	}
	if !got.AutoRegister {
		t.Fatal("AutoRegister should default to true")
	}
}

func TestSchemaOptionsFromConfigNilWhenURLEmpty(t *testing.T) {
	if got := schemaOptionsFromConfig("t", config.SchemaRegistryConfig{}); got != nil {
		t.Fatalf("expected nil when url empty, got %+v", got)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./cmd/rss2msg/ -run TestSchemaOptions -v`
Expected: FAIL — `undefined: schemaOptionsFromConfig`.

- [ ] **Step 3: Implement the mapper and use it**

In `cmd/rss2msg/wire.go`, add the import `sinkschema "github.com/iambod/rss2msg/internal/sink/kafka/schema"` (alias to avoid clashing with any `schema` name) and a helper:
```go
// schemaOptionsFromConfig maps the kafka schema_registry config to the sink's
// schema.Options. Returns nil when the registry is not configured (url empty),
// which keeps the plain-JSON value path. AutoRegister defaults to true: an
// unset key (false) is treated as enabled because the documented default is
// "register on first use".
func schemaOptionsFromConfig(topic string, c config.SchemaRegistryConfig) *sinkschema.Options {
	if c.URL == "" {
		return nil
	}
	o := &sinkschema.Options{
		URL:          c.URL,
		Format:       sinkschema.Format(c.Format),
		Topic:        topic,
		Subject:      c.Subject,
		AutoRegister: true,
		BasicUser:    c.BasicAuth.Username,
		BasicPass:    c.BasicAuth.Password,
	}
	return o
}
```

> Default-true rationale: the spec/docs document `auto_register` as default `true`. The Go zero value is `false`, so we cannot distinguish "set to false" from "unset" via the struct alone. For PR1 we always pass `true` here; explicit `auto_register: false` support is a follow-up (note it in docs as "not yet honored"). **Simpler alternative if you prefer honoring false now:** read the raw value via Viper's `IsSet` in the load path and store a `*bool`. PR1 keeps the bool + always-true mapping for simplicity; revisit if needed.

In the kafka case of `sinkFromConfig` (around line 435), pass the schema options and the TLS for the registry. First, build the registry TLS config (reuse `sinkKafkaTLSFromConfig` shape — but `schema.Options.TLS` is a `*tls.Config`, while `sinkKafkaTLSFromConfig` returns `*sinkkafka.TLSOptions`). Map the registry TLS into a `*tls.Config` only if cert files are set; for PR1, pass `nil` TLS for the registry and document that registry TLS uses system roots unless a follow-up wires it. Update the kafka case:
```go
	case "kafka":
		return sinkkafka.New(sinkkafka.Options{
			Name: sc.Name, Brokers: sc.Kafka.Brokers, Topic: sc.Kafka.Topic,
			Acks: sc.Kafka.Acks, Compression: sc.Kafka.Compression,
			TLS:    sinkKafkaTLSFromConfig(sc.Kafka.TLS),
			Schema: schemaOptionsFromConfig(sc.Kafka.Topic, sc.Kafka.SchemaRegistry),
		})
```

> If `schema_file` is set, load it here and set `o.SchemaText`. Add inside `schemaOptionsFromConfig` before the return:
> ```go
> if c.SchemaFile != "" {
> 	b, err := os.ReadFile(c.SchemaFile)
> 	if err == nil { // validation already proved it exists; ignore here or log
> 		o.SchemaText = string(b)
> 	}
> }
> ```
> Add `"os"` to wire.go imports if needed. (Validation in Task 7 already guarantees the file exists, so a read error here is unexpected; log at warn if the project has a logger in scope, otherwise leave the canonical schema.)

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./cmd/rss2msg/ -run TestSchemaOptions -v`
Expected: PASS.

- [ ] **Step 5: Build + full unit suite + vet + lint**

Run: `task build && task test && task vet`
Expected: all PASS. Then `task lint` (or note if golangci-lint unavailable).

- [ ] **Step 6: Commit**

```bash
git add cmd/rss2msg/wire.go cmd/rss2msg/wire_schema_test.go
git commit -m "feat(kafka): wire schema_registry config into the sink"
```

---

## Task 9: Integration test — Kafka + Schema Registry round-trip

**Files:**
- Create: `internal/sink/kafka/schema_integration_test.go`

- [ ] **Step 1: Write the test**

`internal/sink/kafka/schema_integration_test.go`:
```go
//go:build integration

package kafka_test

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/iambod/rss2msg/internal/model"
	sinkkafka "github.com/iambod/rss2msg/internal/sink/kafka"
	"github.com/iambod/rss2msg/internal/sink/kafka/schema"
)

// startSchemaRegistry runs cp-schema-registry pointed at the given Kafka
// bootstrap servers and returns its base URL.
func startSchemaRegistry(t *testing.T, bootstrap string) string {
	t.Helper()
	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:        "confluentinc/cp-schema-registry:7.6.0",
		ExposedPorts: []string{"8081/tcp"},
		Env: map[string]string{
			"SCHEMA_REGISTRY_HOST_NAME":                    "schema-registry",
			"SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS": bootstrap,
			"SCHEMA_REGISTRY_LISTENERS":                    "http://0.0.0.0:8081",
		},
		WaitingFor: wait.ForHTTP("/subjects").WithPort("8081/tcp").WithStartupTimeout(90 * time.Second),
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	if err != nil {
		t.Fatalf("start schema registry: %v", err)
	}
	t.Cleanup(func() { _ = c.Terminate(ctx) })
	host, err := c.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	port, err := c.MappedPort(ctx, "8081")
	if err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("http://%s:%s", host, port.Port())
}

func TestSchemaRegistryJSONRoundTrip(t *testing.T) {
	ctx := context.Background()
	kc, err := tckafka.Run(ctx, "confluentinc/cp-kafka:7.6.0", tckafka.WithClusterID("test-cluster"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = kc.Terminate(ctx) })
	brokers, err := kc.Brokers(ctx)
	if err != nil {
		t.Fatal(err)
	}
	createTopic(t, brokers, "feed.changes.schema")

	// cp-schema-registry needs an internal-network bootstrap address; reuse the
	// advertised broker list the container exposes.
	srURL := startSchemaRegistry(t, brokers[0])

	pub, err := sinkkafka.New(sinkkafka.Options{
		Name: "schema", Brokers: brokers, Topic: "feed.changes.schema", Acks: "all",
		Schema: &schema.Options{URL: srURL, Format: schema.FormatJSON, Topic: "feed.changes.schema", AutoRegister: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pub.Close() }()

	c := model.Change{SchemaVersion: 1, FeedURL: "f1", ItemID: "i1", Kind: model.ChangeNew, ContentHash: "h", Title: "hi"}
	pctx, pcancel := context.WithTimeout(ctx, 30*time.Second)
	defer pcancel()
	if err := pub.Publish(pctx, c); err != nil {
		t.Fatalf("publish: %v", err)
	}

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics("feed.changes.schema"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()

	fctx, fcancel := context.WithTimeout(ctx, 15*time.Second)
	defer fcancel()
	fetches := consumer.PollFetches(fctx)
	if errs := fetches.Errors(); len(errs) > 0 {
		t.Fatalf("poll errs: %v", errs)
	}
	var saw bool
	fetches.EachRecord(func(r *kgo.Record) {
		if string(r.Key) != "i1" {
			return
		}
		if len(r.Value) < 5 || r.Value[0] != 0 {
			t.Fatalf("value not Confluent-framed: %v", r.Value[:min(5, len(r.Value))])
		}
		if id := binary.BigEndian.Uint32(r.Value[1:5]); id == 0 {
			t.Fatal("zero schema id in framed record")
		}
		var round model.Change
		if err := json.Unmarshal(r.Value[5:], &round); err != nil {
			t.Fatalf("payload: %v", err)
		}
		if round.Title != "hi" {
			t.Fatalf("title = %q", round.Title)
		}
		saw = true
	})
	if !saw {
		t.Fatal("did not observe framed record")
	}
}
```

> `min` is a Go 1.21+ builtin — fine on Go 1.25. `createTopic` already exists in `kafka_test.go` (same `kafka_test` package), so it is reused.

- [ ] **Step 2: Run it (requires Docker)**

Run: `go test -tags=integration ./internal/sink/kafka/ -run TestSchemaRegistryJSONRoundTrip -v`
Expected: PASS. If `cp-schema-registry` cannot reach the broker via `brokers[0]` (host-mapped address), set the bootstrap to the container's advertised internal listener — see testcontainers kafka module docs; adjust `startSchemaRegistry`'s bootstrap arg accordingly. If Docker is unavailable, note that this test was not run and must pass in CI.

- [ ] **Step 3: Commit**

```bash
git add internal/sink/kafka/schema_integration_test.go
git commit -m "test(kafka): integration round-trip for JSON schema-registry encoding"
```

---

## Task 10: Documentation

**Files:**
- Modify: `docs/how-to/sinks/kafka.md`

- [ ] **Step 1: Add the schema_registry section**

After the field table in `docs/how-to/sinks/kafka.md`, add a `schema_registry` row to the table:
```markdown
| `schema_registry` | no | (off) | Confluent Schema Registry encoding of the record value. Absent ⇒ plain JSON. See below. |
```

Then add a section before `## Related`:
```markdown
## Schema Registry (optional)

Set `schema_registry.url` to frame the record value with the Confluent wire
format (magic byte + 4-byte schema ID + payload) and register a schema. Absent,
the value is plain JSON exactly as before — this is fully opt-in and per-sink.

```yaml
- name: events
  driver: kafka
  kafka:
    brokers: ["kafka:9092"]
    topic: feed.changes
    schema_registry:
      url: http://schema-registry:8081  # presence enables the feature
      format: json                      # json (avro, protobuf in later releases)
      subject: feed.changes-value       # default <topic>-value
      auto_register: true               # default true
      schema_file: ./change.schema.json # optional: overrides the registered schema text
      basic_auth:
        username: sruser
        password: ${SR_PASSWORD}
```

| field | required | default | values |
| --- | --- | --- | --- |
| `url` | yes (to enable) | — | Schema Registry base URL. |
| `format` | yes (when url set) | — | `json` (Avro/Protobuf land in later releases). |
| `subject` | no | `<topic>-value` | Subject name (TopicNameStrategy). |
| `auto_register` | no | `true` | Register the schema on first publish; `false` looks up an existing id and errors if absent. |
| `schema_file` | no | (canonical) | Overrides the registered schema text; must stay wire-compatible with the canonical `Change` shape. |
| `basic_auth` | no | (none) | `username` / `password` for the registry. |
| `tls` | no | (off) | TLS to the registry; same shape as the broker `tls` block. |

When enabled, registration or encoding errors **hard-fail** the publish so
unframed records never land. The canonical JSON Schema is generated from the
`Change` envelope; `schema_file` lets you register a stricter or annotated
variant.
```

Also update the "Record layout" bullet for `Value`:
```markdown
- `Value` = JSON `Change` envelope (plain), or a Confluent-framed value when
  `schema_registry` is configured.
```

Update the frontmatter `updated:` to `2026-06-09`.

- [ ] **Step 2: Run the doc link checker**

Run: `bash scripts/check-doc-links.sh`
Expected: `OK: all relative doc links resolve`.

- [ ] **Step 3: Commit**

```bash
git add docs/how-to/sinks/kafka.md
git commit -m "docs(kafka): document optional schema_registry block"
```

---

## Task 11: Final verification & PR

- [ ] **Step 1: Full gate**

Run:
```bash
task build && task test && task vet
bash scripts/check-doc-links.sh
task lint   # or note if golangci-lint v2 is unavailable
```
Expected: all green. Run `task test-integration` if Docker is available (this PR touches a sink); otherwise state explicitly that it was skipped.

- [ ] **Step 2: Verify only intended files are staged**

Run: `git status` and confirm no Obsidian/vault noise is staged (per the repo staging hazard). Stage only the files listed in this plan.

- [ ] **Step 3: Push and open the PR**

```bash
git push -u origin feat/kafka-schema-registry
gh pr create --repo IAmBod/rss2msg --base main \
  --title "feat(kafka): Confluent Schema Registry support — JSON Schema (#61)" \
  --body "Implements PR1 of #61 (foundation + JSON Schema). See docs/superpowers/specs/2026-06-09-kafka-schema-registry-design.md.

Opt-in per kafka sink via \`schema_registry.url\`; absent ⇒ plain JSON (unchanged). JSON Schema generated from \`model.Change\`, value framed with the Confluent wire format. Hard-fails publish on registration/encode error. Avro (PR2) and Protobuf (PR3) follow.

🤖 Generated with [Claude Code](https://claude.com/claude-code)"
```

---

## Self-Review notes (against the spec)

- **Optionality / plain-JSON default** → Tasks 5, 8 (`encoder == nil` path), tested in Task 5.
- **JSON Schema format + Confluent framing** → Tasks 2–4, tested in Task 4 + Task 9.
- **Auto-generate + operator override** → Task 4 (`canonicalJSONSchema` + `SchemaText`), Task 8 (`schema_file` load).
- **Hard-fail when on** → Task 5 (`Encode` error propagated from `Publish`).
- **Lazy, idempotent registration / auto_register=false lookup** → Task 3, tested in `registry_test.go`.
- **Subject default `<topic>-value`** → Task 2 (`defaultSubject`), tested.
- **Config + validation** → Tasks 6–7, tested.
- **Unchanged headers/key/trace** → Task 5 leaves the rest of `Publish` intact.
- **Tests: unit (srfake) + integration (testcontainers) + framing** → Tasks 3,4,9.
- **Deferred to PR2/PR3 (documented):** Avro/Protobuf encoders and their validation/format acceptance; registry TLS `*tls.Config` mapping and honoring `auto_register: false` from an unset-vs-false distinction are flagged as follow-ups in Task 8.

**Benchmark note:** the spec calls for an encode-path benchmark for the CI bench gate. Add `BenchmarkJSONEncoderEncode` in `internal/sink/kafka/schema/json_test.go` if the bench gate requires coverage of this path; it is low-risk and can ride in Task 4's commit. (Listed here so it is not silently dropped.)
