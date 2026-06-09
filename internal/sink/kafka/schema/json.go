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
