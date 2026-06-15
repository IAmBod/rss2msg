// Package schema adds opt-in Confluent Schema Registry support to the Kafka
// sink. An Encoder frames a model.Change into the Confluent wire format
// (magic byte + 4-byte big-endian schema ID + payload) after registering the
// schema with a Schema Registry. When no schema_registry block is configured
// the kafka sink does not construct an Encoder and emits plain JSON as before.
package schema

import (
	"context"
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
	TLS          *TLSOptions
}

// New builds an Encoder for the configured format.
func New(opts Options) (Encoder, error) {
	if opts.URL == "" {
		return nil, fmt.Errorf("schema registry: url is required")
	}
	if opts.Topic == "" && opts.Subject == "" {
		return nil, fmt.Errorf("schema registry: topic or subject is required")
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
