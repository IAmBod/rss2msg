package composite

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/iambod/rss2msg/internal/model"
	"github.com/iambod/rss2msg/internal/retry"
	"github.com/iambod/rss2msg/internal/sink"
)

// fakeSink records the changes it receives and optionally fails.
type fakeSink struct {
	name     string
	failWith error
	mu       sync.Mutex
	received []model.Change
	closed   int
}

func (f *fakeSink) Name() string { return f.name }
func (f *fakeSink) Close() error { f.mu.Lock(); defer f.mu.Unlock(); f.closed++; return nil }
func (f *fakeSink) Publish(_ context.Context, c model.Change) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.received = append(f.received, c)
	return f.failWith
}
func (f *fakeSink) count() int { f.mu.Lock(); defer f.mu.Unlock(); return len(f.received) }

func branch(name string, primary, dlq sink.Publisher) Branch {
	return Branch{Name: name, Wrapped: sink.WithRetry(primary, dlq, retry.Config{MaxAttempts: 1})}
}

func sampleChange() model.Change {
	return model.Change{
		SchemaVersion: model.SchemaVersion, FeedURL: "https://e/feed", ItemID: "i1",
		Kind: model.ChangeNew, ContentHash: "deadbeef",
		DetectedAt: time.Date(2026, 5, 31, 12, 0, 0, 0, time.UTC),
	}
}

func TestNewRejectsMissingName(t *testing.T) {
	t.Parallel()
	if _, err := New(Options{}); err == nil {
		t.Fatal("expected error for missing name")
	}
}

func TestPublishFansOutToAllChildren(t *testing.T) {
	t.Parallel()
	a, b := &fakeSink{name: "a"}, &fakeSink{name: "b"}
	p, err := New(Options{Name: "fanout"})
	if err != nil {
		t.Fatal(err)
	}
	p.SetBranches([]Branch{branch("a", a, nil), branch("b", b, nil)})
	if err := p.Publish(context.Background(), sampleChange()); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	if a.count() != 1 || b.count() != 1 {
		t.Fatalf("each child should receive once: a=%d b=%d", a.count(), b.count())
	}
}

func TestPublishReportsDroppedChildAndStillDeliversOthers(t *testing.T) {
	t.Parallel()
	good := &fakeSink{name: "good"}
	bad := &fakeSink{name: "bad", failWith: errors.New("boom")} // no DLQ -> dropped
	p, _ := New(Options{Name: "fanout"})
	p.SetBranches([]Branch{branch("good", good, nil), branch("bad", bad, nil)})
	err := p.Publish(context.Background(), sampleChange())
	if err == nil || !strings.Contains(err.Error(), "bad") {
		t.Fatalf("error must name dropped child, got %v", err)
	}
	if good.count() != 1 {
		t.Fatalf("healthy child should still receive: good=%d", good.count())
	}
}

func TestPublishChildCapturedByDLQReturnsNil(t *testing.T) {
	t.Parallel()
	bad := &fakeSink{name: "bad", failWith: errors.New("boom")}
	dlq := &fakeSink{name: "dlq"}
	p, _ := New(Options{Name: "fanout"})
	p.SetBranches([]Branch{branch("bad", bad, dlq)})
	if err := p.Publish(context.Background(), sampleChange()); err != nil {
		t.Fatalf("child captured by DLQ must not fail the composite, got %v", err)
	}
	if dlq.count() != 1 {
		t.Fatalf("DLQ should receive the envelope: dlq=%d", dlq.count())
	}
	if dlq.received[0].DLQFromSink != "bad" {
		t.Fatalf("envelope should be annotated, got %q", dlq.received[0].DLQFromSink)
	}
}

func TestCloseDoesNotCloseChildren(t *testing.T) {
	t.Parallel()
	a := &fakeSink{name: "a"}
	p, _ := New(Options{Name: "fanout"})
	p.SetBranches([]Branch{branch("a", a, nil)})
	if err := p.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if a.closed != 0 {
		t.Fatalf("composite must not close children, closed=%d", a.closed)
	}
}

func TestPublishRecordsMetrics(t *testing.T) {
	t.Parallel()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	p, err := New(Options{Name: "fanout", Meter: mp.Meter("test")})
	if err != nil {
		t.Fatal(err)
	}
	p.SetBranches([]Branch{branch("a", &fakeSink{name: "a"}, nil)})
	if err := p.Publish(context.Background(), sampleChange()); err != nil {
		t.Fatal(err)
	}
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	if len(rm.ScopeMetrics) == 0 {
		t.Fatal("expected metrics recorded")
	}
}

func TestPublishIsConcurrencySafe(t *testing.T) {
	t.Parallel()
	// One composite instance is shared across feeds publishing concurrently.
	// The branch slice is read-only after SetBranches, so this must be race-free
	// (run with -race) and every child must receive every change exactly once.
	a, b := &fakeSink{name: "a"}, &fakeSink{name: "b"}
	p, _ := New(Options{Name: "fanout"})
	p.SetBranches([]Branch{branch("a", a, nil), branch("b", b, nil)})
	const n = 100
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := p.Publish(context.Background(), sampleChange()); err != nil {
				t.Errorf("publish: %v", err)
			}
		}()
	}
	wg.Wait()
	if a.count() != n || b.count() != n {
		t.Fatalf("each child should receive %d: a=%d b=%d", n, a.count(), b.count())
	}
}
