package sink

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/iambod/rss2msg/internal/model"
	"github.com/iambod/rss2msg/internal/retry"
)

func TestRetryWrapSucceedsSkipsDLQ(t *testing.T) {
	t.Parallel()
	primary := &fakePub{name: "primary"}
	dlq := &fakePub{name: "dlq"}
	w := WithRetry(primary, dlq, retry.Config{MaxAttempts: 3, BaseDelay: time.Millisecond, MaxDelay: 2 * time.Millisecond}, 0)

	res := w.Deliver(context.Background(), model.Change{ItemID: "1"})
	if res.State != BranchSuccess {
		t.Fatalf("got state %v", res.State)
	}
	if len(primary.published) != 1 || len(dlq.published) != 0 {
		t.Fatalf("unexpected sends primary=%d dlq=%d", len(primary.published), len(dlq.published))
	}
}

func TestRetryWrapDLQCapturesOnFinalFailure(t *testing.T) {
	t.Parallel()
	primary := &fakePub{name: "primary", err: errors.New("boom")}
	dlq := &fakePub{name: "dlq"}
	w := WithRetry(primary, dlq, retry.Config{MaxAttempts: 2, BaseDelay: time.Millisecond, MaxDelay: 2 * time.Millisecond}, 0)

	res := w.Deliver(context.Background(), model.Change{ItemID: "1"})
	if res.State != BranchDLQ {
		t.Fatalf("got state %v err=%v", res.State, res.Err)
	}
	if len(dlq.published) != 1 {
		t.Fatalf("dlq expected 1, got %d", len(dlq.published))
	}
	envelope := dlq.published[0]
	if envelope.DLQFromSink != "primary" || envelope.DLQAttempts != 2 {
		t.Fatalf("dlq envelope missing context: %+v", envelope)
	}
	if envelope.DLQError == "" {
		t.Fatalf("expected dlq error message")
	}
}

func TestRetryWrapDropsWhenNoDLQAndPrimaryFails(t *testing.T) {
	t.Parallel()
	primary := &fakePub{name: "primary", err: errors.New("boom")}
	w := WithRetry(primary, nil, retry.Config{MaxAttempts: 2, BaseDelay: time.Millisecond, MaxDelay: 2 * time.Millisecond}, 0)

	res := w.Deliver(context.Background(), model.Change{ItemID: "1"})
	if res.State != BranchDropped {
		t.Fatalf("got state %v", res.State)
	}
}

func TestRetryWrapDropsWhenDLQAlsoFails(t *testing.T) {
	t.Parallel()
	primary := &fakePub{name: "primary", err: errors.New("primary boom")}
	dlq := &fakePub{name: "dlq", err: errors.New("dlq boom")}
	w := WithRetry(primary, dlq, retry.Config{MaxAttempts: 2, BaseDelay: time.Millisecond, MaxDelay: 2 * time.Millisecond}, 0)

	res := w.Deliver(context.Background(), model.Change{ItemID: "1"})
	if res.State != BranchDropped {
		t.Fatalf("got state %v", res.State)
	}
}

// blockingPub blocks in Publish until the context is cancelled, or 5s elapses
// as a safety net so a missing timeout never wedges the suite.
type blockingPub struct{ name string }

func (b *blockingPub) Name() string { return b.name }
func (b *blockingPub) Publish(ctx context.Context, _ model.Change) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(5 * time.Second):
		return nil
	}
}
func (b *blockingPub) Close() error { return nil }

func TestRetryWrapDeliverTimeoutBoundsAWedgedSink(t *testing.T) {
	t.Parallel()
	primary := &blockingPub{name: "primary"}
	// MaxAttempts:1 so the wedge is a single hung Publish, not retry backoff.
	w := WithRetry(primary, nil, retry.Config{MaxAttempts: 1}, 50*time.Millisecond)

	start := time.Now()
	res := w.Deliver(context.Background(), model.Change{ItemID: "1"})
	elapsed := time.Since(start)

	if elapsed > time.Second {
		t.Fatalf("Deliver did not honor deliver timeout: took %v", elapsed)
	}
	if res.State != BranchDropped {
		t.Fatalf("expected BranchDropped on timeout, got %v", res.State)
	}
}

func TestRetryWrapZeroTimeoutDoesNotCancel(t *testing.T) {
	t.Parallel()
	// timeout=0 means "off": a fast primary still succeeds, no deadline imposed.
	primary := &fakePub{name: "primary"}
	w := WithRetry(primary, nil, retry.Config{MaxAttempts: 1}, 0)

	res := w.Deliver(context.Background(), model.Change{ItemID: "1"})
	if res.State != BranchSuccess {
		t.Fatalf("expected success with timeout off, got %v err=%v", res.State, res.Err)
	}
}

func TestRetryWrapTruncatesLongDLQError(t *testing.T) {
	t.Parallel()
	big := make([]byte, 2000)
	for i := range big {
		big[i] = 'x'
	}
	primary := &fakePub{name: "primary", err: errors.New(string(big))}
	dlq := &fakePub{name: "dlq"}
	w := WithRetry(primary, dlq, retry.Config{MaxAttempts: 1, BaseDelay: time.Millisecond, MaxDelay: time.Millisecond}, 0)
	w.Deliver(context.Background(), model.Change{ItemID: "1"})
	if got := len(dlq.published[0].DLQError); got > 1024 {
		t.Fatalf("expected truncation to 1 KiB, got %d", got)
	}
}
