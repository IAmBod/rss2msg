// Package memory provides an in-process Coordinator that always grants the
// lease. Used for single-instance deployments and as the default when no
// coordinator is configured.
package memory

import (
	"context"

	"github.com/iambod/rss2msg/internal/coord"
)

type Coordinator struct{}

func New() *Coordinator { return &Coordinator{} }

func (Coordinator) TryAcquire(ctx context.Context, feedURL string) (coord.ReleaseFunc, bool, error) {
	return release, true, nil
}

func (Coordinator) Close() error { return nil }

func release(ctx context.Context) error { return nil }
