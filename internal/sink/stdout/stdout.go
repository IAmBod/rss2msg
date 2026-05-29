// Package stdout implements a sink.Publisher that writes each Change as a
// newline-delimited JSON record to stdout or stderr. Intended for local
// development, debugging, and ad-hoc pipelines (`./rss2msg run-once … | jq`).
//
// Concurrent publishes are serialised with a mutex so feeds polling in
// parallel never interleave bytes mid-record.
package stdout

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/iambod/rss2msg/internal/model"
)

// Options configures a stdout Publisher.
type Options struct {
	Name string // sink name (required)

	// Target selects the destination stream. "stdout" (default) | "stderr".
	Target string

	// Format selects the encoding. "json" (default) writes one Change per
	// line (NDJSON). "pretty" indents the JSON with two spaces, which is
	// readable in a terminal but no longer machine-parseable line-by-line.
	Format string
}

var (
	validTargets = map[string]struct{}{
		"":       {},
		"stdout": {},
		"stderr": {},
	}
	validFormats = map[string]struct{}{
		"":       {},
		"json":   {},
		"pretty": {},
	}
)

type Publisher struct {
	name   string
	w      io.Writer
	pretty bool

	mu sync.Mutex
}

// New constructs a Publisher that writes to os.Stdout / os.Stderr per
// opts.Target. For tests, use NewForWriter to inject an arbitrary writer.
func New(opts Options) (*Publisher, error) {
	if opts.Name == "" {
		return nil, fmt.Errorf("stdout sink: name is required")
	}
	if _, ok := validTargets[opts.Target]; !ok {
		return nil, fmt.Errorf("stdout sink %q: unknown target %q (want stdout or stderr)", opts.Name, opts.Target)
	}
	if _, ok := validFormats[opts.Format]; !ok {
		return nil, fmt.Errorf("stdout sink %q: unknown format %q (want json or pretty)", opts.Name, opts.Format)
	}
	var w io.Writer = os.Stdout
	if opts.Target == "stderr" {
		w = os.Stderr
	}
	return &Publisher{name: opts.Name, w: w, pretty: opts.Format == "pretty"}, nil
}

// NewForWriter is the test-facing constructor: emit to an arbitrary writer.
// Production callers use New.
func NewForWriter(name string, w io.Writer, pretty bool) *Publisher {
	return &Publisher{name: name, w: w, pretty: pretty}
}

func (p *Publisher) Name() string { return p.name }

func (p *Publisher) Close() error { return nil }

func (p *Publisher) Publish(ctx context.Context, change model.Change) error {
	var body []byte
	var err error
	if p.pretty {
		body, err = json.MarshalIndent(change, "", "  ")
	} else {
		body, err = json.Marshal(change)
	}
	if err != nil {
		return fmt.Errorf("stdout sink %q: marshal: %w", p.name, err)
	}
	// Single Write to keep records intact under concurrent publishers; the
	// mutex serialises Go-level interleaving even when the underlying writer
	// doesn't guarantee write atomicity.
	body = append(body, '\n')

	p.mu.Lock()
	defer p.mu.Unlock()
	if _, err := p.w.Write(body); err != nil {
		return fmt.Errorf("stdout sink %q: write: %w", p.name, err)
	}
	return nil
}
