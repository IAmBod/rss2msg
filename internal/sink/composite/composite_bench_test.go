package composite

import (
	"context"
	"fmt"
	"testing"

	"github.com/iambod/rss2msg/internal/model"
)

// benchSink is a no-op child that records nothing, so fan-out benchmarks measure
// the composite's per-child dispatch cost without the unbounded slice growth the
// test fakeSink would incur over millions of iterations.
type benchSink struct{ n string }

func (s benchSink) Name() string                                { return s.n }
func (s benchSink) Close() error                                { return nil }
func (s benchSink) Publish(context.Context, model.Change) error { return nil }

// benchFanout measures Publish dispatching one change across `children` branches.
func benchFanout(b *testing.B, children int) {
	p, err := New(Options{Name: "bench"})
	if err != nil {
		b.Fatal(err)
	}
	branches := make([]Branch, children)
	for i := range branches {
		name := fmt.Sprintf("c%d", i)
		branches[i] = branch(name, benchSink{n: name}, nil)
	}
	p.SetBranches(branches)

	change := sampleChange()
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := p.Publish(ctx, change); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCompositeFanout1(b *testing.B)  { benchFanout(b, 1) }
func BenchmarkCompositeFanout4(b *testing.B)  { benchFanout(b, 4) }
func BenchmarkCompositeFanout16(b *testing.B) { benchFanout(b, 16) }
