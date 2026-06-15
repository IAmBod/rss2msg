package schema

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/sr"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/iambod/rss2msg/internal/model"
	sinkv1 "github.com/iambod/rss2msg/proto/sink/v1"
)

// changeProtoSchema is the canonical Protobuf schema registered for model.Change.
// It is a standalone proto3 file whose Change message field numbers match the
// generated sinkv1.Change, so the proto.Marshal bytes decode against it.
const changeProtoSchema = `syntax = "proto3";

package rss2msg.sink.v1;

import "google/protobuf/timestamp.proto";

message Change {
  int32 schema_version = 1;
  string feed_url = 2;
  string feed_title = 3;
  string item_id = 4;
  string kind = 5;
  string title = 6;
  string link = 7;
  repeated string authors = 8;
  string summary = 9;
  string content = 10;
  repeated string categories = 11;
  google.protobuf.Timestamp published_at = 12;
  google.protobuf.Timestamp updated_at = 13;
  string content_hash = 14;
  google.protobuf.Timestamp detected_at = 15;
  string dlq_from_sink = 16;
  string dlq_error = 17;
  int32 dlq_attempts = 18;
}
`

// ChangeProtoSchema returns the canonical Protobuf schema text for model.Change.
func ChangeProtoSchema() string { return changeProtoSchema }

func toProtoChange(c model.Change) *sinkv1.Change {
	pc := &sinkv1.Change{
		SchemaVersion: int32(c.SchemaVersion),
		FeedUrl:       c.FeedURL,
		FeedTitle:     c.FeedTitle,
		ItemId:        c.ItemID,
		Kind:          string(c.Kind),
		Title:         c.Title,
		Link:          c.Link,
		Authors:       c.Authors,
		Summary:       c.Summary,
		Content:       c.Content,
		Categories:    c.Categories,
		ContentHash:   c.ContentHash,
		DetectedAt:    timestamppb.New(c.DetectedAt),
		DlqFromSink:   c.DLQFromSink,
		DlqError:      c.DLQError,
		DlqAttempts:   int32(c.DLQAttempts),
	}
	if c.PublishedAt != nil {
		pc.PublishedAt = timestamppb.New(*c.PublishedAt)
	}
	if c.UpdatedAt != nil {
		pc.UpdatedAt = timestamppb.New(*c.UpdatedAt)
	}
	return pc
}

type protobufEncoder struct {
	reg    *registrar
	header sr.ConfluentHeader
}

func newProtobufEncoder(opts Options, subject string) (Encoder, error) {
	cl, err := newClient(opts)
	if err != nil {
		return nil, fmt.Errorf("schema registry client: %w", err)
	}
	text := opts.SchemaText
	if text == "" {
		text = changeProtoSchema
	}
	return &protobufEncoder{
		reg: &registrar{
			cl:      cl,
			subject: subject,
			schema:  sr.Schema{Schema: text, Type: sr.TypeProtobuf},
			auto:    opts.AutoRegister,
		},
	}, nil
}

func (e *protobufEncoder) Format() string { return string(FormatProtobuf) }

func (e *protobufEncoder) Encode(ctx context.Context, c model.Change) ([]byte, error) {
	id, err := e.reg.schemaID(ctx)
	if err != nil {
		return nil, fmt.Errorf("schema registry: %w", err)
	}
	payload, err := proto.Marshal(toProtoChange(c))
	if err != nil {
		return nil, fmt.Errorf("marshal protobuf change: %w", err)
	}
	// Single top-level message → message-index [0] (encoded as one 0 byte).
	buf, _ := e.header.AppendEncode(nil, id, []int{0})
	return append(buf, payload...), nil
}
