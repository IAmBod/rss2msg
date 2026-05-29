package memory

import (
	"context"
	"testing"
)

func TestMemoryAlwaysAcquires(t *testing.T) {
	t.Parallel()
	c := New()
	release, acquired, err := c.TryAcquire(context.Background(), "https://e/feed-1")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !acquired {
		t.Fatal("expected acquired=true")
	}
	if release == nil {
		t.Fatal("expected non-nil release")
	}
	if err := release(context.Background()); err != nil {
		t.Fatalf("release err: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("close err: %v", err)
	}
}
