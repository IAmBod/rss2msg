package http

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/iambod/rss2msg/internal/model"
)

func sampleChange() model.Change {
	return model.Change{
		SchemaVersion: model.SchemaVersion,
		FeedURL:       "https://e/feed",
		ItemID:        "i1",
		Kind:          model.ChangeNew,
		Title:         "Hello world",
		ContentHash:   "deadbeef",
		DetectedAt:    time.Date(2026, 5, 30, 12, 0, 0, 0, time.UTC),
	}
}

func TestNewRejectsMissingName(t *testing.T) {
	t.Parallel()
	if _, err := New(Options{URL: "https://example/h"}); err == nil {
		t.Fatal("expected error for missing name")
	}
}

func TestNewRejectsMissingURL(t *testing.T) {
	t.Parallel()
	if _, err := New(Options{Name: "x"}); err == nil {
		t.Fatal("expected error for missing url")
	}
}

func TestNewRejectsBadURL(t *testing.T) {
	t.Parallel()
	for _, u := range []string{"not a url", "ftp://example/h", "https:///", "//example/h"} {
		if _, err := New(Options{Name: "x", URL: u}); err == nil {
			t.Errorf("expected error for url %q", u)
		}
	}
}

func TestNewRejectsBadMethod(t *testing.T) {
	t.Parallel()
	if _, err := New(Options{Name: "x", URL: "https://e/h", Method: "DELETE"}); err == nil {
		t.Fatal("expected error for DELETE")
	}
}

func TestNewRejectsBadSuccessCode(t *testing.T) {
	t.Parallel()
	if _, err := New(Options{Name: "x", URL: "https://e/h", SuccessCodes: []int{99}}); err == nil {
		t.Fatal("expected error for code below 100")
	}
	if _, err := New(Options{Name: "x", URL: "https://e/h", SuccessCodes: []int{600}}); err == nil {
		t.Fatal("expected error for code above 599")
	}
}

type recordedReq struct {
	method  string
	path    string
	headers http.Header
	body    []byte
}

// recordingServer returns a test server that captures the first request it
// receives onto the returned channel, and replies with the given status.
func recordingServer(t *testing.T, status int) (*httptest.Server, <-chan recordedReq) {
	t.Helper()
	ch := make(chan recordedReq, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		select {
		case ch <- recordedReq{method: r.Method, path: r.URL.Path, headers: r.Header.Clone(), body: body}:
		default:
		}
		w.WriteHeader(status)
	}))
	t.Cleanup(srv.Close)
	return srv, ch
}

func TestPublishPOSTsJSONWithCanonicalHeaders(t *testing.T) {
	t.Parallel()
	srv, reqs := recordingServer(t, 200)

	p, err := New(Options{Name: "test", URL: srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err := p.Publish(context.Background(), sampleChange()); err != nil {
		t.Fatalf("publish: %v", err)
	}

	r := <-reqs
	if r.method != http.MethodPost {
		t.Errorf("method: want POST, got %s", r.method)
	}
	if r.headers.Get("Content-Type") != "application/json" {
		t.Errorf("content-type: %q", r.headers.Get("Content-Type"))
	}
	if r.headers.Get("X-Feed-Url") != "https://e/feed" {
		t.Errorf("X-Feed-Url: %q", r.headers.Get("X-Feed-Url"))
	}
	if r.headers.Get("X-Item-Id") != "i1" {
		t.Errorf("X-Item-Id: %q", r.headers.Get("X-Item-Id"))
	}
	if r.headers.Get("X-Kind") != "new" {
		t.Errorf("X-Kind: %q", r.headers.Get("X-Kind"))
	}
	if r.headers.Get("X-Schema-Version") != "1" {
		t.Errorf("X-Schema-Version: %q", r.headers.Get("X-Schema-Version"))
	}

	var round model.Change
	if err := json.Unmarshal(r.body, &round); err != nil {
		t.Fatalf("body parse: %v\n%s", err, r.body)
	}
	if round.ItemID != "i1" || round.Title != "Hello world" {
		t.Errorf("body round-trip: %+v", round)
	}
}

func TestPublishMethodOverridePUT(t *testing.T) {
	t.Parallel()
	srv, reqs := recordingServer(t, 200)

	p, err := New(Options{Name: "test", URL: srv.URL, Method: "PUT"})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err := p.Publish(context.Background(), sampleChange()); err != nil {
		t.Fatal(err)
	}
	r := <-reqs
	if r.method != http.MethodPut {
		t.Errorf("method: want PUT, got %s", r.method)
	}
}

func TestPublishStaticHeadersApplied(t *testing.T) {
	t.Parallel()
	srv, reqs := recordingServer(t, 200)

	p, err := New(Options{
		Name: "test", URL: srv.URL,
		Headers: map[string]string{"Authorization": "Bearer abc", "X-Source": "rss2msg"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err := p.Publish(context.Background(), sampleChange()); err != nil {
		t.Fatal(err)
	}
	r := <-reqs
	if r.headers.Get("Authorization") != "Bearer abc" {
		t.Errorf("Authorization: %q", r.headers.Get("Authorization"))
	}
	if r.headers.Get("X-Source") != "rss2msg" {
		t.Errorf("X-Source: %q", r.headers.Get("X-Source"))
	}
}

func TestPublishCanonicalHeadersWinOverStaticOverrides(t *testing.T) {
	t.Parallel()
	srv, reqs := recordingServer(t, 200)

	// Operator tries to clobber per-record metadata with a static header.
	// Canonical headers must still reflect the actual Change.
	p, err := New(Options{
		Name: "test", URL: srv.URL,
		Headers: map[string]string{
			"X-Item-Id":    "WRONG",
			"X-Feed-Url":   "WRONG",
			"Content-Type": "text/plain",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err := p.Publish(context.Background(), sampleChange()); err != nil {
		t.Fatal(err)
	}
	r := <-reqs
	if r.headers.Get("X-Item-Id") != "i1" {
		t.Errorf("static X-Item-Id leaked through: %q", r.headers.Get("X-Item-Id"))
	}
	if r.headers.Get("X-Feed-Url") != "https://e/feed" {
		t.Errorf("static X-Feed-Url leaked through: %q", r.headers.Get("X-Feed-Url"))
	}
	if r.headers.Get("Content-Type") != "application/json" {
		t.Errorf("Content-Type was overridden: %q", r.headers.Get("Content-Type"))
	}
}

func TestPublishDLQHeadersWhenPopulated(t *testing.T) {
	t.Parallel()
	srv, reqs := recordingServer(t, 200)

	p, err := New(Options{Name: "test", URL: srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	c := sampleChange()
	c.DLQFromSink = "kafka-main"
	c.DLQError = "broker unreachable"
	c.DLQAttempts = 3
	if err := p.Publish(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	r := <-reqs
	if r.headers.Get("X-Dlq-From-Sink") != "kafka-main" {
		t.Errorf("X-Dlq-From-Sink: %q", r.headers.Get("X-Dlq-From-Sink"))
	}
	if r.headers.Get("X-Dlq-Error") != "broker unreachable" {
		t.Errorf("X-Dlq-Error: %q", r.headers.Get("X-Dlq-Error"))
	}
	if r.headers.Get("X-Dlq-Attempts") != "3" {
		t.Errorf("X-Dlq-Attempts: %q", r.headers.Get("X-Dlq-Attempts"))
	}
}

func TestPublishNonSuccessReturnsError(t *testing.T) {
	t.Parallel()
	srv, _ := recordingServer(t, 500)

	p, err := New(Options{Name: "test", URL: srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	err = p.Publish(context.Background(), sampleChange())
	if err == nil || !strings.Contains(err.Error(), "500") {
		t.Fatalf("expected error containing 500, got %v", err)
	}
}

func TestPublishCustomSuccessCodes(t *testing.T) {
	t.Parallel()
	srv, _ := recordingServer(t, 418) // I'm a teapot — non-default but operator wants it accepted

	p, err := New(Options{Name: "test", URL: srv.URL, SuccessCodes: []int{200, 418}})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err := p.Publish(context.Background(), sampleChange()); err != nil {
		t.Fatalf("expected 418 to be treated as success, got %v", err)
	}
}

func TestPublishContextCancelAbortsRequest(t *testing.T) {
	t.Parallel()
	// Handler waits for the request context to be done (which fires when
	// the client closes its end after ctx cancel) but also has a hard 2s
	// fallback so srv.Close() can't deadlock on a stuck handler if the
	// connection close doesn't propagate.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(2 * time.Second):
		}
	}))
	defer srv.Close()

	p, err := New(Options{Name: "test", URL: srv.URL, Timeout: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(50 * time.Millisecond); cancel() }()
	err = p.Publish(ctx, sampleChange())
	if err == nil {
		t.Fatal("expected error from canceled ctx")
	}
}

func TestPublishConcurrent(t *testing.T) {
	t.Parallel()
	var count atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count.Add(1)
		w.WriteHeader(200)
	}))
	defer srv.Close()

	p, err := New(Options{Name: "test", URL: srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	const n = 50
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
	if got := count.Load(); got != n {
		t.Fatalf("expected %d requests, got %d", n, got)
	}
}

func TestPublishInjectsTraceparentWhenSpanActive(t *testing.T) {
	srv, reqs := recordingServer(t, 200)

	// Snapshot the globals before mutating so other tests (or future tests)
	// don't inherit the TraceContext propagator we install here.
	prevProp := otel.GetTextMapPropagator()
	prevTP := otel.GetTracerProvider()
	otel.SetTextMapPropagator(propagation.TraceContext{})
	tp := sdktrace.NewTracerProvider()
	otel.SetTracerProvider(tp)
	defer func() {
		_ = tp.Shutdown(context.Background())
		otel.SetTracerProvider(prevTP)
		otel.SetTextMapPropagator(prevProp)
	}()

	p, err := New(Options{Name: "test", URL: srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	ctx, span := otel.Tracer("test").Start(context.Background(), "publish.test")
	defer span.End()
	if err := p.Publish(ctx, sampleChange()); err != nil {
		t.Fatal(err)
	}
	r := <-reqs
	if r.headers.Get("Traceparent") == "" {
		t.Errorf("expected traceparent header injected, got %v", r.headers)
	}
}
