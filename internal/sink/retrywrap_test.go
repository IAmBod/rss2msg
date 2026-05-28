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
	w := WithRetry(primary, dlq, retry.Config{MaxAttempts: 3, BaseDelay: time.Millisecond, MaxDelay: 2 * time.Millisecond})

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
	w := WithRetry(primary, dlq, retry.Config{MaxAttempts: 2, BaseDelay: time.Millisecond, MaxDelay: 2 * time.Millisecond})

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
	w := WithRetry(primary, nil, retry.Config{MaxAttempts: 2, BaseDelay: time.Millisecond, MaxDelay: 2 * time.Millisecond})

	res := w.Deliver(context.Background(), model.Change{ItemID: "1"})
	if res.State != BranchDropped {
		t.Fatalf("got state %v", res.State)
	}
}

func TestRetryWrapDropsWhenDLQAlsoFails(t *testing.T) {
	t.Parallel()
	primary := &fakePub{name: "primary", err: errors.New("primary boom")}
	dlq := &fakePub{name: "dlq", err: errors.New("dlq boom")}
	w := WithRetry(primary, dlq, retry.Config{MaxAttempts: 2, BaseDelay: time.Millisecond, MaxDelay: 2 * time.Millisecond})

	res := w.Deliver(context.Background(), model.Change{ItemID: "1"})
	if res.State != BranchDropped {
		t.Fatalf("got state %v", res.State)
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
	w := WithRetry(primary, dlq, retry.Config{MaxAttempts: 1, BaseDelay: time.Millisecond, MaxDelay: time.Millisecond})
	w.Deliver(context.Background(), model.Change{ItemID: "1"})
	if got := len(dlq.published[0].DLQError); got > 1024 {
		t.Fatalf("expected truncation to 1 KiB, got %d", got)
	}
}
