# Kafka Schema Registry support — design

- **Issue:** [#61](https://github.com/IAmBod/rss2msg/issues/61) "Kafka schema support"
- **Status:** approved design, pending implementation
- **Date:** 2026-06-09

## Problem

The Kafka sink (`internal/sink/kafka/kafka.go`) serializes `model.Change` as plain
JSON into the record value, with metadata in headers. Consumers in a Confluent-style
ecosystem (Kafka Connect, ksqlDB, Confluent deserializers, schema-governed pipelines)
expect records framed with the **Confluent Schema Registry wire format** and a schema
registered in a Schema Registry. There is no way to produce schema-registered records
today.

## Goal

Add **opt-in** Confluent Schema Registry support to the Kafka sink, covering the three
standard serialization formats — **JSON Schema, Avro, and Protobuf** — with
auto-generated canonical schemas and an operator override. When unconfigured, behavior
is byte-for-byte identical to today (plain JSON).

## Decisions (locked)

| Decision | Choice |
| --- | --- |
| Formats | JSON Schema **and** Avro **and** Protobuf |
| Schema source | Auto-generated canonical schema per format; operator may override the registered schema text via `schema_file` |
| Failure mode | Opt-in; **hard-fail** when enabled — any registration/encode error propagates out of `Publish` so unframed/bad records never land |
| Registry client | `github.com/twmb/franz-go/pkg/sr` (same ecosystem as the existing `kgo` producer, pure-Go, no cgo) |

### Library trade-off (recorded)

- **(A) franz-go `pkg/sr`** — chosen. Composes with the existing `kgo` client, pure Go,
  ships Confluent wire-framing helpers.
- (B) `confluent-kafka-go` — rejected: cgo/librdkafka, would duplicate the producer.
- (C) hand-rolled registry REST client — rejected: more code to own for no benefit.

## Architecture

A pluggable encoder behind one interface, selected by config. When
`schema_registry` is absent the sink takes today's exact `json.Marshal(Change)` path —
no encoder is constructed.

```go
// internal/sink/kafka/schema
type Encoder interface {
    // Encode returns the Confluent-framed record value for a Change.
    Encode(ctx context.Context, c model.Change) ([]byte, error)
    // Format reports the wire format name ("json" | "avro" | "protobuf").
    Format() string
}
```

### Registration

- Lazy, on first publish, via the `sr` client; the resolved schema ID is cached for the
  publisher's lifetime. If registration fails it is retried on the next publish (the
  failing publish hard-fails) until it succeeds, then cached. Lazy registration keeps a
  registry blip at startup from blocking process boot while still failing closed.
- `auto_register: true` (default) → `CreateSchema(subject, schema)` (idempotent
  server-side; returns the existing ID when the schema is identical).
- `auto_register: false` → look up an existing ID for the subject; error if absent.

### Wire format

`[magic byte 0x00][4-byte big-endian schema ID][payload]`. Protobuf additionally
prepends the message-index varint array between the schema ID and the payload, per the
Confluent Protobuf spec.

### Subject naming

Default **TopicNameStrategy**: `<topic>-value`. Overridable via `subject`.

### Unchanged across all formats

- Record key stays `change.ItemID`.
- Headers (`feed_url`, `kind`, `schema_version`, the DLQ headers, and W3C
  `traceparent`/`tracestate` trace context) are emitted exactly as today.

## Per-format encoders

| Format | Schema | Payload encoding | Dependency |
| --- | --- | --- | --- |
| `json` | JSON Schema generated from `model.Change` (`github.com/google/jsonschema-go`, already an indirect dep) | `json.Marshal(Change)` — identical bytes to today, just framed | none new |
| `avro` | Canonical embedded `.avsc` for `Change` | `github.com/hamba/avro/v2` binary encode | `hamba/avro/v2` (new) |
| `protobuf` | Reuse existing `proto/sink/v1.Change`; register its file descriptor | `model.Change` → `sinkv1.Change` → `proto.Marshal` | existing `google.golang.org/protobuf` + `proto/sink/v1` |

### Schema source / override semantics

- Default: built-in canonical schema per format (embedded for Avro/Protobuf; reflected
  for JSON Schema).
- `schema_file`: overrides the **registered schema text** only. The byte encoding always
  derives from `model.Change`. Docs note that an override must remain wire-compatible
  with the canonical shape (intended for namespace/doc/subject tweaks, not field-shape
  changes). This is documented as an advanced option.

## Config surface (additive, opt-in)

```yaml
sinks:
  - name: events
    driver: kafka
    kafka:
      brokers: [kafka:9092]
      topic: feed.changes
      schema_registry:                    # absent ⇒ current plain-JSON behavior
        url: http://schema-registry:8081  # presence enables the feature
        format: json | avro | protobuf    # required when url set
        subject: feed.changes-value       # default <topic>-value
        auto_register: true               # default true
        schema_file: ./schemas/change.avsc # optional override of registered schema text
        basic_auth:
          username: sruser
          password: ${SR_PASSWORD}
        tls: { }                          # reuse SinkTLSConfig (ca_file/cert_file/key_file/server_name/insecure_skip_verify)
```

`KafkaSinkConfig` gains a `SchemaRegistry SchemaRegistryConfig` field
(`mapstructure:"schema_registry"`).

## Error handling

Opt-in, hard-fail when on: any registration or encode error is returned from `Publish`.
`insecure_skip_verify` is logged at `warn`, matching the existing TLS pattern.

## Validation (`internal/config/validate.go`)

- `schema_registry.url` set ⇒ `format` required and in `{json, avro, protobuf}`.
- `format` set without `url` ⇒ error.
- `schema_file`, if set, must exist and be readable.
- `basic_auth` (both username and password) and `tls` validated like the existing sink
  TLS/auth blocks.

## Dependencies to add

- `github.com/twmb/franz-go/pkg/sr` — Schema Registry client + Confluent wire framing.
- `github.com/hamba/avro/v2` — Avro encoding (PR2 only).
- JSON Schema: `github.com/google/jsonschema-go` (promote from indirect).
- Protobuf: existing `google.golang.org/protobuf` + `proto/sink/v1`.

## Testing

- **Unit:** wire-framing (magic byte + ID, protobuf message-index), per-format encoder
  output, config validation, registration against an `httptest` fake registry.
- **Integration (`-tags=integration`):** testcontainers Kafka + Confluent Schema
  Registry; produce → consume → registry-deserialize round-trip for each format.
- **Benchmark:** encode-path benchmark so the CI bench-regression gate covers schema
  encoding.

## Decomposition (one spec, three PRs)

Per the repo's one-PR-per-task convention, this spec covers all three formats but lands
incrementally, each on its own branch/worktree:

1. **PR1 — foundation + JSON Schema.** `Encoder` interface, `sr` client wiring, config
   (`SchemaRegistryConfig`) + validation, the `json` encoder, unit + integration tests,
   and docs. This commits the spec doc.
2. **PR2 — Avro.** `avro` encoder + `hamba/avro/v2` dep + canonical `.avsc` + tests.
3. **PR3 — Protobuf.** `protobuf` encoder reusing `proto/sink/v1.Change` +
   message-index framing + file-descriptor registration + tests.

## Out of scope

- KeyNameStrategy / RecordNameStrategy / TopicRecordNameStrategy subject strategies
  (only TopicNameStrategy, with explicit `subject` override).
- Schema evolution/compatibility management beyond what auto-registration provides.
- Deserialization (rss2msg is a producer; the feed sink already consumes plain JSON).
- Reflection-based schema generation for Avro/Protobuf (canonical embedded schemas;
  override via file).
