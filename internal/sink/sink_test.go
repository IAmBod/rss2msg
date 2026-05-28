package sink

import (
	"context"
	"testing"

	"github.com/iambod/rss2msg/internal/model"
)

type fakePub struct {
	name      string
	published []model.Change
	err       error
}

func (f *fakePub) Name() string { return f.name }
func (f *fakePub) Publish(ctx context.Context, c model.Change) error {
	if f.err != nil {
		return f.err
	}
	f.published = append(f.published, c)
	return nil
}
func (f *fakePub) Close() error { return nil }

func TestRegistryAddAndGet(t *testing.T) {
	t.Parallel()
	r := NewRegistry()
	p := &fakePub{name: "a"}
	if err := r.Add(p); err != nil {
		t.Fatal(err)
	}
	if err := r.Add(p); err == nil {
		t.Fatalf("expected duplicate error")
	}
	got, ok := r.Get("a")
	if !ok || got != p {
		t.Fatalf("registry round-trip failed")
	}
	if _, ok := r.Get("missing"); ok {
		t.Fatalf("unexpected hit for missing")
	}
}
