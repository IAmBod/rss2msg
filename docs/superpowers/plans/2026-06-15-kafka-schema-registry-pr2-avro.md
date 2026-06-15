# Kafka Schema Registry — PR2 (Avro) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Add the **Avro** wire format to the Kafka sink's Schema Registry support, on the `Encoder` foundation shipped in PR1 (#159).

**Architecture:** A new `avroEncoder` in `internal/sink/kafka/schema` registers a canonical embedded Avro schema for `model.Change` (Type `sr.TypeAvro`) and frames `avro.Marshal`-encoded bytes with the same Confluent wire format. A dedicated `avroChange` struct with `avro:` tags + a mapper keeps `model.Change` untouched (mirroring the proto approach). Format selection, lazy/cached registration, hard-fail, registry TLS/auth all reuse PR1.

**Tech Stack:** `github.com/hamba/avro/v2` (struct tags, `timestamp-micros` logical types ↔ `time.Time`, `["null",X]` unions ↔ pointers), `franz-go/pkg/sr` (registration + `ConfluentHeader`).

**Scope:** Avro only. Protobuf is PR3. Spec: `docs/superpowers/specs/2026-06-09-kafka-schema-registry-design.md`.

---

## File Structure

**Create:**
- `internal/sink/kafka/schema/avro.go` — embedded `.avsc`, `avroChange` struct + mapper, `avroEncoder`, `newAvroEncoder`.
- `internal/sink/kafka/schema/avro_test.go` — unit tests (parse, frame, round-trip, format).

**Modify:**
- `internal/sink/kafka/schema/encoder.go` — `New` switch: route `FormatAvro` to `newAvroEncoder`; leave only `FormatProtobuf` in the "not supported yet" arm.
- `internal/sink/kafka/schema/encoder_test.go` — `TestNewRejectsUnsupportedFormat` must use `"protobuf"` now (avro is supported).
- `internal/config/validate.go` — accept `avro`; reject only `protobuf` as not-yet.
- `internal/config/validate_test.go` — the avro case now succeeds; add/keep a protobuf-rejected case.
- `internal/sink/kafka/schema_integration_test.go` — add an Avro round-trip test.
- `docs/how-to/sinks/kafka.md` — format values `json | avro` (protobuf planned).
- `go.mod` / `go.sum` — add `github.com/hamba/avro/v2`.

---

## Task 1: Avro encoder, schema, and mapper

**Files:** Create `internal/sink/kafka/schema/avro.go` + `avro_test.go`.

- [ ] **Step 1: Write the failing test** — `internal/sink/kafka/schema/avro_test.go`:
```go
package schema

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/hamba/avro/v2"
	"github.com/twmb/franz-go/pkg/sr/srfake"

	"github.com/iambod/rss2msg/internal/model"
)

func TestCanonicalAvroSchemaParses(t *testing.T) {
	if _, err := avro.Parse(changeAvroSchema); err != nil {
		t.Fatalf("canonical avro schema does not parse: %v", err)
	}
}

func TestAvroEncoderFramesAndRoundTrips(t *testing.T) {
	fake := srfake.New()
	t.Cleanup(fake.Close)

	enc, err := New(Options{URL: fake.URL(), Format: FormatAvro, Topic: "feed.changes", AutoRegister: true})
	if err != nil {
		t.Fatal(err)
	}
	if enc.Format() != "avro" {
		t.Fatalf("Format() = %q, want avro", enc.Format())
	}

	det := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)
	pub := time.Date(2026, 6, 14, 9, 30, 0, 0, time.UTC)
	c := model.Change{
		SchemaVersion: 1, FeedURL: "f1", ItemID: "i1", Kind: model.ChangeNew,
		Title: "hi", Authors: []string{"a"}, Categories: []string{"x"},
		PublishedAt: &pub, ContentHash: "h", DetectedAt: det,
	}
	framed, err := enc.Encode(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	if len(framed) < 5 || framed[0] != 0 {
		t.Fatalf("not confluent-framed: %v", framed[:min(5, len(framed))])
	}
	if binary.BigEndian.Uint32(framed[1:5]) == 0 {
		t.Fatal("zero schema id")
	}

	parsed := avro.MustParse(changeAvroSchema)
	var round avroChange
	if err := avro.Unmarshal(parsed, framed[5:], &round); err != nil {
		t.Fatalf("avro payload did not decode: %v", err)
	}
	if round.Title != "hi" || round.FeedURL != "f1" {
		t.Fatalf("round-trip mismatch: %+v", round)
	}
	if !round.DetectedAt.Equal(det) {
		t.Fatalf("detected_at = %v, want %v", round.DetectedAt, det)
	}
	if round.PublishedAt == nil || !round.PublishedAt.Equal(pub) {
		t.Fatalf("published_at = %v, want %v", round.PublishedAt, pub)
	}
	if len(round.Authors) != 1 || round.Authors[0] != "a" {
		t.Fatalf("authors = %v", round.Authors)
	}
}

func TestAvroEncoderNilOptionalTimeEncodesNull(t *testing.T) {
	fake := srfake.New()
	t.Cleanup(fake.Close)
	enc, err := New(Options{URL: fake.URL(), Format: FormatAvro, Topic: "t", AutoRegister: true})
	if err != nil {
		t.Fatal(err)
	}
	det := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)
	framed, err := enc.Encode(context.Background(), model.Change{ItemID: "i", ContentHash: "h", DetectedAt: det})
	if err != nil {
		t.Fatal(err)
	}
	parsed := avro.MustParse(changeAvroSchema)
	var round avroChange
	if err := avro.Unmarshal(parsed, framed[5:], &round); err != nil {
		t.Fatal(err)
	}
	if round.PublishedAt != nil || round.UpdatedAt != nil {
		t.Fatalf("nil optional times should decode to nil, got pub=%v upd=%v", round.PublishedAt, round.UpdatedAt)
	}
}
```

- [ ] **Step 2: Run, expect FAIL** — `go test ./internal/sink/kafka/schema/ -run Avro -v` → undefined `changeAvroSchema`, `avroChange`, no avro route.

- [ ] **Step 3: Implement** — `internal/sink/kafka/schema/avro.go`:
```go
package schema

import (
	"context"
	"fmt"
	"time"

	"github.com/hamba/avro/v2"
	"github.com/twmb/franz-go/pkg/sr"

	"github.com/iambod/rss2msg/internal/model"
)

// changeAvroSchema is the canonical Avro schema for model.Change. Optional
// scalars carry Avro defaults so the record is forward-compatible; optional
// times are nullable unions; required times use timestamp-micros logical type.
const changeAvroSchema = `{
  "type": "record",
  "name": "Change",
  "namespace": "rss2msg",
  "fields": [
    {"name": "schema_version", "type": "long"},
    {"name": "feed_url", "type": "string"},
    {"name": "feed_title", "type": "string", "default": ""},
    {"name": "item_id", "type": "string"},
    {"name": "kind", "type": "string"},
    {"name": "title", "type": "string", "default": ""},
    {"name": "link", "type": "string", "default": ""},
    {"name": "authors", "type": {"type": "array", "items": "string"}, "default": []},
    {"name": "summary", "type": "string", "default": ""},
    {"name": "content", "type": "string", "default": ""},
    {"name": "categories", "type": {"type": "array", "items": "string"}, "default": []},
    {"name": "published_at", "type": ["null", {"type": "long", "logicalType": "timestamp-micros"}], "default": null},
    {"name": "updated_at", "type": ["null", {"type": "long", "logicalType": "timestamp-micros"}], "default": null},
    {"name": "content_hash", "type": "string"},
    {"name": "detected_at", "type": {"type": "long", "logicalType": "timestamp-micros"}},
    {"name": "dlq_from_sink", "type": "string", "default": ""},
    {"name": "dlq_error", "type": "string", "default": ""},
    {"name": "dlq_attempts", "type": "long", "default": 0}
  ]
}`

// avroChange mirrors model.Change with avro tags for hamba/avro. model.Change
// is left untouched (it carries json tags for the JSON encoder).
type avroChange struct {
	SchemaVersion int        `avro:"schema_version"`
	FeedURL       string     `avro:"feed_url"`
	FeedTitle     string     `avro:"feed_title"`
	ItemID        string     `avro:"item_id"`
	Kind          string     `avro:"kind"`
	Title         string     `avro:"title"`
	Link          string     `avro:"link"`
	Authors       []string   `avro:"authors"`
	Summary       string     `avro:"summary"`
	Content       string     `avro:"content"`
	Categories    []string   `avro:"categories"`
	PublishedAt   *time.Time `avro:"published_at"`
	UpdatedAt     *time.Time `avro:"updated_at"`
	ContentHash   string     `avro:"content_hash"`
	DetectedAt    time.Time  `avro:"detected_at"`
	DLQFromSink   string     `avro:"dlq_from_sink"`
	DLQError      string     `avro:"dlq_error"`
	DLQAttempts   int        `avro:"dlq_attempts"`
}

func toAvroChange(c model.Change) avroChange {
	return avroChange{
		SchemaVersion: c.SchemaVersion,
		FeedURL:       c.FeedURL,
		FeedTitle:     c.FeedTitle,
		ItemID:        c.ItemID,
		Kind:          string(c.Kind),
		Title:         c.Title,
		Link:          c.Link,
		Authors:       c.Authors,
		Summary:       c.Summary,
		Content:       c.Content,
		Categories:    c.Categories,
		PublishedAt:   c.PublishedAt,
		UpdatedAt:     c.UpdatedAt,
		ContentHash:   c.ContentHash,
		DetectedAt:    c.DetectedAt,
		DLQFromSink:   c.DLQFromSink,
		DLQError:      c.DLQError,
		DLQAttempts:   c.DLQAttempts,
	}
}

type avroEncoder struct {
	reg    *registrar
	header sr.ConfluentHeader
	schema avro.Schema
}

func newAvroEncoder(opts Options, subject string) (Encoder, error) {
	cl, err := newClient(opts)
	if err != nil {
		return nil, fmt.Errorf("schema registry client: %w", err)
	}
	text := opts.SchemaText
	if text == "" {
		text = changeAvroSchema
	}
	parsed, err := avro.Parse(text)
	if err != nil {
		return nil, fmt.Errorf("parse avro schema: %w", err)
	}
	return &avroEncoder{
		reg: &registrar{
			cl:      cl,
			subject: subject,
			schema:  sr.Schema{Schema: text, Type: sr.TypeAvro},
			auto:    opts.AutoRegister,
		},
		schema: parsed,
	}, nil
}

func (e *avroEncoder) Format() string { return string(FormatAvro) }

func (e *avroEncoder) Encode(ctx context.Context, c model.Change) ([]byte, error) {
	id, err := e.reg.schemaID(ctx)
	if err != nil {
		return nil, fmt.Errorf("schema registry: %w", err)
	}
	payload, err := avro.Marshal(e.schema, toAvroChange(c))
	if err != nil {
		return nil, fmt.Errorf("marshal avro change: %w", err)
	}
	buf, _ := e.header.AppendEncode(nil, id, nil) // error is always nil
	return append(buf, payload...), nil
}
```

- [ ] **Step 4: Route the format** — in `internal/sink/kafka/schema/encoder.go` `New`, change:
```go
	case FormatJSON:
		return newJSONEncoder(opts, subject)
	case FormatAvro:
		return newAvroEncoder(opts, subject)
	case FormatProtobuf:
		return nil, fmt.Errorf("schema registry: format %q is not supported yet (have %q, %q)", opts.Format, FormatJSON, FormatAvro)
	default:
		return nil, fmt.Errorf("schema registry: unknown format %q", opts.Format)
	}
```

- [ ] **Step 5: Fix the now-stale test** — in `internal/sink/kafka/schema/encoder_test.go`, `TestNewRejectsUnsupportedFormat` currently passes `Format: "avro"`. Change it to `Format: "protobuf"` (avro is now supported; protobuf is not).

- [ ] **Step 6: Run** — `go get github.com/hamba/avro/v2 && go mod tidy && go test ./internal/sink/kafka/schema/ -v` → all pass.

- [ ] **Step 7: vet + lint** — `go vet ./internal/sink/kafka/schema/ && golangci-lint run ./internal/sink/kafka/schema/`.

- [ ] **Step 8: Commit** —
```bash
git add internal/sink/kafka/schema/avro.go internal/sink/kafka/schema/avro_test.go internal/sink/kafka/schema/encoder.go internal/sink/kafka/schema/encoder_test.go go.mod go.sum
git commit -m "feat(kafka): add Avro schema-registry encoder"
```

---

## Task 2: Validation accepts avro

**Files:** `internal/config/validate.go`, `internal/config/validate_test.go`.

- [ ] **Step 1: Update the test first** — in `TestValidateSchemaRegistry` (validate_test.go): the `Format: "avro"` case currently expects an error. Change it to expect SUCCESS:
```go
	if _, err := Validate(base(SchemaRegistryConfig{URL: "http://sr:8081", Format: "avro"})); err != nil {
		t.Fatalf("valid avro registry rejected: %v", err)
	}
```
Keep the `protobuf` case expecting an error (it already exists). Run `go test ./internal/config/ -run TestValidateSchemaRegistry -v` → FAIL (avro currently rejected).

- [ ] **Step 2: Update validation** — in `internal/config/validate.go`, the format switch in the schema-registry loop changes to:
```go
		switch sr.Format {
		case "json", "avro":
			// supported in this release
		case "protobuf":
			return *warnings, fmt.Errorf("sinks[%d] (kafka %q): schema_registry.format %q is not supported yet (json, avro)", i, s.Name, sr.Format)
		case "":
			return *warnings, fmt.Errorf("sinks[%d] (kafka %q): schema_registry.format is required when url is set", i, s.Name)
		default:
			return *warnings, fmt.Errorf("sinks[%d] (kafka %q): schema_registry.format %q is invalid (json|avro|protobuf)", i, s.Name, sr.Format)
		}
```

- [ ] **Step 3: Run** — `go test ./internal/config/... -v -run TestValidateSchemaRegistry` → pass. Then full `go test ./internal/config/...`.

- [ ] **Step 4: Commit** —
```bash
git add internal/config/validate.go internal/config/validate_test.go
git commit -m "feat(config): accept avro schema_registry format"
```

---

## Task 3: Integration round-trip for Avro

**Files:** `internal/sink/kafka/schema_integration_test.go` (append a test).

- [ ] **Step 1: Add the test** (same package `kafka_test`, build tag already present). Reuse `startSchemaRegistry`, the kafka container pattern, and `createTopic` from the existing file. Decode with the canonical Avro schema:
```go
func TestSchemaRegistryAvroRoundTrip(t *testing.T) {
	ctx := context.Background()
	nw, err := tcnetwork.New(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = nw.Remove(ctx) })
	kc, err := tckafka.Run(ctx, "confluentinc/cp-kafka:7.6.0",
		tckafka.WithClusterID("test-cluster"),
		tcnetwork.WithNetwork([]string{kafkaAlias}, nw),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = kc.Terminate(ctx) })
	brokers, err := kc.Brokers(ctx)
	if err != nil {
		t.Fatal(err)
	}
	createTopic(t, brokers, "feed.changes.avro")
	srURL := startSchemaRegistry(t, kafkaAlias+":9092", nw)

	pub, err := sinkkafka.New(sinkkafka.Options{
		Name: "avro", Brokers: brokers, Topic: "feed.changes.avro", Acks: "all",
		Schema: &schema.Options{URL: srURL, Format: schema.FormatAvro, Topic: "feed.changes.avro", AutoRegister: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pub.Close() }()

	det := time.Now().UTC().Truncate(time.Microsecond)
	c := model.Change{SchemaVersion: 1, FeedURL: "f1", ItemID: "i1", Kind: model.ChangeNew, ContentHash: "h", Title: "hi", DetectedAt: det}
	pctx, pcancel := context.WithTimeout(ctx, 30*time.Second)
	defer pcancel()
	if err := pub.Publish(pctx, c); err != nil {
		t.Fatalf("publish: %v", err)
	}

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics("feed.changes.avro"),
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
	parsed := avro.MustParse(schema.ChangeAvroSchema())
	var saw bool
	fetches.EachRecord(func(r *kgo.Record) {
		if string(r.Key) != "i1" {
			return
		}
		if len(r.Value) < 5 || r.Value[0] != 0 {
			t.Fatalf("value not confluent-framed: %v", r.Value)
		}
		var round struct {
			Title       string    `avro:"title"`
			FeedURL     string    `avro:"feed_url"`
			DetectedAt  time.Time `avro:"detected_at"`
		}
		_ = round
		var full map[string]any
		if err := avro.Unmarshal(parsed, r.Value[5:], &full); err != nil {
			t.Fatalf("avro decode: %v", err)
		}
		if full["title"] != "hi" || full["feed_url"] != "f1" {
			t.Fatalf("decoded mismatch: %+v", full)
		}
		saw = true
	})
	if !saw {
		t.Fatal("did not observe framed avro record")
	}
}
```
> NOTE: this references `schema.ChangeAvroSchema()` (an exported accessor) and `kafkaAlias` + a `startSchemaRegistry(url, network)` signature. Inspect the EXISTING `schema_integration_test.go` to match the actual `startSchemaRegistry` signature and `kafkaAlias`/network setup it already uses (PR1's integration test created a shared network). Adapt the kafka-start + `startSchemaRegistry` call to the real helpers rather than the sketch above. For decoding, either add a tiny exported `schema.ChangeAvroSchema() string` returning `changeAvroSchema` (preferred — add it in `avro.go`), or inline a literal copy of the schema in the test. If you add the exported accessor, include it in Task 1's commit instead and note it.

- [ ] **Step 2: Compile under the tag** — `go vet -tags=integration ./internal/sink/kafka/`. If Docker is available, run `go test -tags=integration ./internal/sink/kafka/ -run 'TestSchemaRegistryAvroRoundTrip' -v -timeout 300s` and report. If not, report compile-only.

- [ ] **Step 3: Commit** —
```bash
git add internal/sink/kafka/schema_integration_test.go internal/sink/kafka/schema/avro.go
git commit -m "test(kafka): integration round-trip for Avro schema-registry encoding"
```

---

## Task 4: Docs

**Files:** `docs/how-to/sinks/kafka.md`.

- [ ] **Step 1: Update** the `## Schema Registry (optional)` section: change every `format` mention from "json only" to "`json` or `avro` (`protobuf` planned)". Specifically the YAML comment (`format: json` → `format: json   # or avro (protobuf planned)`) and the field table row for `format` (`json` → `json` \| `avro`; protobuf rejected). Bump frontmatter `updated:` to `2026-06-15`. Add one sentence noting Avro uses a canonical schema with `timestamp-micros` logical types and nullable optional times.

- [ ] **Step 2: Link check** — `bash scripts/check-doc-links.sh` → `OK: all relative doc links resolve`.

- [ ] **Step 3: Commit** —
```bash
git add docs/how-to/sinks/kafka.md
git commit -m "docs(kafka): document avro schema_registry format"
```

---

## Task 5: Final gate + PR

- [ ] `task build && task test && task vet`; `golangci-lint run ./...` (changed pkgs at minimum); `bash scripts/check-doc-links.sh`. Run `task test-integration` if Docker available (touches a sink) — else say it was skipped.
- [ ] `git status` — only intended files.
- [ ] Push `feat/kafka-schema-avro`; open PR titled `feat(kafka): Schema Registry Avro format (#61)` referencing PR1 (#159) as the foundation and noting Protobuf (PR3) remains.

---

## Self-Review (against spec)

- Avro format + canonical schema + Confluent framing → Task 1, tested unit + integration.
- Operator override (`schema_file`) → reused from PR1 (`opts.SchemaText` honored in `newAvroEncoder`).
- Hard-fail on register/encode error → `Encode` propagates; covered by the foundation.
- `model.Change` untouched (avro tags live on `avroChange`) → Task 1.
- Validation/docs updated; stale PR1 tests (`encoder_test` avro→protobuf, `validate_test` avro now valid) fixed → Tasks 1, 2.
- Timestamps: `timestamp-micros`; optional times nullable → schema + round-trip tests assert micros equality and nil→null.
- Protobuf still deferred (PR3).
