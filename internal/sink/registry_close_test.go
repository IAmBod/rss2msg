package sink

import (
	"context"
	"errors"
	"sort"
	"testing"

	"github.com/iambod/rss2msg/internal/model"
)

// closerPub is a Publisher whose Close result is scriptable, so the
// Registry.Close aggregation can be exercised. (sink_test.go's fakePub always
// closes nil.)
type closerPub struct {
	name     string
	closeErr error
	closed   bool
}

func (p *closerPub) Name() string                                { return p.name }
func (p *closerPub) Publish(context.Context, model.Change) error { return nil }
func (p *closerPub) Close() error {
	p.closed = true
	return p.closeErr
}

func TestRegistryAllReturnsEveryPublisher(t *testing.T) {
	t.Parallel()
	r := NewRegistry()
	for _, n := range []string{"a", "b", "c"} {
		if err := r.Add(&fakePub{name: n}); err != nil {
			t.Fatalf("add %q: %v", n, err)
		}
	}
	all := r.All()
	if len(all) != 3 {
		t.Fatalf("All() returned %d publishers, want 3", len(all))
	}
	names := make([]string, 0, len(all))
	for _, p := range all {
		names = append(names, p.Name())
	}
	sort.Strings(names)
	want := []string{"a", "b", "c"}
	for i := range want {
		if names[i] != want[i] {
			t.Fatalf("All() names = %v, want %v", names, want)
		}
	}
}

func TestRegistryAllEmpty(t *testing.T) {
	t.Parallel()
	if got := NewRegistry().All(); len(got) != 0 {
		t.Fatalf("All() on empty registry = %v, want empty", got)
	}
}

func TestRegistryCloseClosesEveryPublisher(t *testing.T) {
	t.Parallel()
	r := NewRegistry()
	a, b := &closerPub{name: "a"}, &closerPub{name: "b"}
	_ = r.Add(a)
	_ = r.Add(b)
	if err := r.Close(); err != nil {
		t.Fatalf("Close() = %v, want nil", err)
	}
	if !a.closed || !b.closed {
		t.Fatalf("Close() did not close every publisher: a=%v b=%v", a.closed, b.closed)
	}
}

func TestRegistryCloseReturnsFirstErrorButClosesAll(t *testing.T) {
	t.Parallel()
	r := NewRegistry()
	boom := errors.New("boom")
	bad := &closerPub{name: "bad", closeErr: boom}
	good := &closerPub{name: "good"}
	_ = r.Add(bad)
	_ = r.Add(good)

	err := r.Close()
	if !errors.Is(err, boom) {
		t.Fatalf("Close() = %v, want %v", err, boom)
	}
	// A failing Close must not stop the others from being closed.
	if !good.closed {
		t.Fatalf("Close() left a healthy publisher unclosed")
	}
}

func TestBranchStateString(t *testing.T) {
	t.Parallel()
	cases := map[BranchState]string{
		BranchSuccess:    "success",
		BranchDLQ:        "dlq",
		BranchDropped:    "dropped",
		BranchState(999): "dropped", // unknown states fall through to "dropped"
	}
	for state, want := range cases {
		if got := state.String(); got != want {
			t.Fatalf("BranchState(%d).String() = %q, want %q", state, got, want)
		}
	}
}
