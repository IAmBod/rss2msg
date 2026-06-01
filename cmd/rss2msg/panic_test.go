package main

import (
	"context"
	"sync"
	"testing"
	"time"

	sentry "github.com/getsentry/sentry-go"
)

type recordingTransport struct {
	mu     sync.Mutex
	events []*sentry.Event
}

func (r *recordingTransport) Configure(sentry.ClientOptions) {}
func (r *recordingTransport) SendEvent(e *sentry.Event) {
	r.mu.Lock()
	r.events = append(r.events, e)
	r.mu.Unlock()
}
func (r *recordingTransport) Flush(time.Duration) bool              { return true }
func (r *recordingTransport) FlushWithContext(context.Context) bool { return true }
func (r *recordingTransport) Close()                                {}
func (r *recordingTransport) count() int                            { r.mu.Lock(); defer r.mu.Unlock(); return len(r.events) }

func TestReportPanicCapturesEvent(t *testing.T) {
	rt := &recordingTransport{}
	if err := sentry.Init(sentry.ClientOptions{
		Dsn:        "https://public@example.com/1",
		Transport:  rt,
		SampleRate: 1.0,
	}); err != nil {
		t.Fatalf("sentry.Init: %v", err)
	}
	t.Cleanup(func() { _ = sentry.Init(sentry.ClientOptions{}) })

	reportPanic("kaboom")

	if got := rt.count(); got != 1 {
		t.Fatalf("expected 1 captured panic event, got %d", got)
	}
}

func TestReportPanicNilIsNoop(t *testing.T) {
	// Must not panic or block even with no Sentry client configured.
	reportPanic(nil)
}
