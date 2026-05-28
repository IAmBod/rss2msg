package sink

import (
	"context"
	"fmt"

	"github.com/iambod/rss2msg/internal/model"
	"github.com/iambod/rss2msg/internal/retry"
)

type BranchState int

const (
	BranchSuccess BranchState = iota
	BranchDLQ
	BranchDropped
)

func (b BranchState) String() string {
	switch b {
	case BranchSuccess:
		return "success"
	case BranchDLQ:
		return "dlq"
	default:
		return "dropped"
	}
}

type BranchResult struct {
	State    BranchState
	Attempts int
	Err      error // populated when State==BranchDropped or BranchDLQ
}

const dlqErrorMaxBytes = 1024

// RetryingPublisher wraps a primary Publisher with exponential-backoff retry,
// then optionally hands failures to a DLQ Publisher.
type RetryingPublisher struct {
	primary Publisher
	dlq     Publisher // may be nil
	cfg     retry.Config
}

func WithRetry(primary, dlq Publisher, cfg retry.Config) *RetryingPublisher {
	return &RetryingPublisher{primary: primary, dlq: dlq, cfg: cfg}
}

// Deliver runs primary.Publish under retry. On final failure, hands the
// change to dlq (once, no retry). DLQ failures are dropped.
func (r *RetryingPublisher) Deliver(ctx context.Context, change model.Change) BranchResult {
	res := retry.Do(ctx, r.cfg, func(ctx context.Context) error {
		return r.primary.Publish(ctx, change)
	})
	if res.Err == nil {
		return BranchResult{State: BranchSuccess, Attempts: res.Attempts}
	}
	if r.dlq == nil {
		return BranchResult{State: BranchDropped, Attempts: res.Attempts, Err: res.Err}
	}

	envelope := change
	envelope.DLQFromSink = r.primary.Name()
	envelope.DLQError = truncate(res.Err.Error(), dlqErrorMaxBytes)
	envelope.DLQAttempts = res.Attempts

	if err := r.dlq.Publish(ctx, envelope); err != nil {
		return BranchResult{State: BranchDropped, Attempts: res.Attempts, Err: fmt.Errorf("primary err=%w; dlq err=%v", res.Err, err)}
	}
	return BranchResult{State: BranchDLQ, Attempts: res.Attempts, Err: res.Err}
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max]
}
