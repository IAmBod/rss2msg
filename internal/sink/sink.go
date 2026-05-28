package sink

import (
	"context"
	"fmt"

	"github.com/iambod/rss2msg/internal/model"
)

type Publisher interface {
	Name() string
	Publish(ctx context.Context, change model.Change) error
	Close() error
}

type Registry struct {
	byName map[string]Publisher
}

func NewRegistry() *Registry {
	return &Registry{byName: map[string]Publisher{}}
}

func (r *Registry) Add(p Publisher) error {
	if _, exists := r.byName[p.Name()]; exists {
		return fmt.Errorf("duplicate publisher %q", p.Name())
	}
	r.byName[p.Name()] = p
	return nil
}

func (r *Registry) Get(name string) (Publisher, bool) {
	p, ok := r.byName[name]
	return p, ok
}

func (r *Registry) Close() error {
	var firstErr error
	for _, p := range r.byName {
		if err := p.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (r *Registry) All() []Publisher {
	out := make([]Publisher, 0, len(r.byName))
	for _, p := range r.byName {
		out = append(out, p)
	}
	return out
}
