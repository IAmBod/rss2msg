// Package composite implements a sink.Publisher that fans every Change out to a
// list of child sinks. A composite is a transparent fan-out: it adds no retry
// or dead-letter of its own. Each child is wrapped exactly as a sink referenced
// directly by a feed (its own retry budget and dead_letter), so a child reached
// through a composite behaves identically to one referenced directly.
//
// Children are top-level registered sinks owned by the sink Registry, so Close
// is a no-op here to avoid double-closing them.
package composite

import (
	"context"
	"fmt"
	"strings"

	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/iambod/rss2msg/internal/model"
	"github.com/iambod/rss2msg/internal/sink"
)

// Branch is a single child of a composite: a named sink already wrapped with
// its own retry + dead-letter (or wrapped pass-through, when the child is
// itself a composite).
type Branch struct {
	Name    string
	Wrapped *sink.RetryingPublisher
}

// Options configures a composite Publisher.
type Options struct {
	Name     string         // sink name (required)
	Children []string       // child sink names, for diagnostics
	Logger   zerolog.Logger // structured logging; zero value is fine
	Meter    metric.Meter   // optional; nil => no metrics
}

var _ sink.Publisher = (*Publisher)(nil)

// Publisher fans a Change out to its child branches.
type Publisher struct {
	name     string
	children []string
	branches []Branch
	logger   zerolog.Logger
	instr    *instruments
}

// New constructs a composite shell. Branches are attached later via SetBranches
// once every sink has been built and registered (see the wiring link pass).
func New(o Options) (*Publisher, error) {
	if o.Name == "" {
		return nil, fmt.Errorf("composite sink: name is required")
	}
	p := &Publisher{name: o.Name, children: o.Children, logger: o.Logger}
	if o.Meter != nil {
		instr, err := newInstruments(o.Meter)
		if err != nil {
			return nil, fmt.Errorf("composite sink %q: instruments: %w", o.Name, err)
		}
		p.instr = instr
	}
	p.logger.Debug().Str("sink", p.name).Strs("children", p.children).Msg("composite sink configured")
	return p, nil
}

// SetBranches attaches the resolved, wrapped child branches. Called once during
// wiring; the branch slice is read-only afterwards, so Publish is safe for
// concurrent use across feeds.
func (p *Publisher) SetBranches(b []Branch) { p.branches = b }

func (p *Publisher) Name() string { return p.name }

// Close is a no-op: children are registered sinks closed by the Registry.
func (p *Publisher) Close() error { return nil }

// Publish delivers change to every child sequentially. It returns nil when each
// child either succeeded or was captured by its own dead-letter, and an error
// naming the children that were dropped (failed with no dead-letter) otherwise.
func (p *Publisher) Publish(ctx context.Context, change model.Change) error {
	if p.instr != nil {
		p.instr.publishes.Add(ctx, 1, metric.WithAttributes(attribute.String("sink.name", p.name)))
	}
	var dropped []string
	for _, b := range p.branches {
		r := b.Wrapped.Deliver(ctx, change)
		outcome := "success"
		switch r.State {
		case sink.BranchSuccess:
			p.logger.Debug().Str("sink", p.name).Str("child", b.Name).Str("item_id", change.ItemID).Msg("composite child published")
		case sink.BranchDLQ:
			outcome = "dlq"
			p.logger.Warn().Err(r.Err).Str("sink", p.name).Str("child", b.Name).Str("item_id", change.ItemID).Int("attempts", r.Attempts).Msg("composite child captured by DLQ")
		case sink.BranchDropped:
			outcome = "dropped"
			dropped = append(dropped, b.Name)
			p.logger.Error().Err(r.Err).Str("sink", p.name).Str("child", b.Name).Str("item_id", change.ItemID).Int("attempts", r.Attempts).Msg("composite child dropped")
		}
		if p.instr != nil {
			p.instr.children.Add(ctx, 1, metric.WithAttributes(
				attribute.String("sink.name", p.name),
				attribute.String("child", b.Name),
				attribute.String("outcome", outcome),
			))
		}
	}
	if len(dropped) > 0 {
		return fmt.Errorf("composite sink %q: %d child sink(s) dropped: %s", p.name, len(dropped), strings.Join(dropped, ", "))
	}
	return nil
}
