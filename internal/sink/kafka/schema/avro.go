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
// scalars carry Avro defaults; optional times are nullable unions; required
// times use the timestamp-micros logical type.
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

// ChangeAvroSchema returns the canonical Avro schema text for model.Change.
func ChangeAvroSchema() string { return changeAvroSchema }

// avroChange mirrors model.Change with avro tags for hamba/avro; model.Change
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
