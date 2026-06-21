package feed

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

const sampleRSS = `<?xml version="1.0"?>
<rss version="2.0"><channel>
<title>Sample</title>
<item><guid>a</guid><title>Hello</title><link>https://e/a</link><description>body</description></item>
</channel></rss>`

func TestFetchSendsUserAgentAndCustomHeaders(t *testing.T) {
	t.Parallel()
	var gotUA, gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotUA = r.Header.Get("User-Agent")
		gotAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/rss+xml")
		w.Header().Set("ETag", `"v1"`)
		w.Header().Set("Last-Modified", "Mon, 02 Jan 2006 15:04:05 GMT")
		_, _ = w.Write([]byte(sampleRSS))
	}))
	defer srv.Close()

	f := NewFetcher(Options{UserAgent: "rss2msg/test"})
	res, err := f.Fetch(context.Background(), FetchRequest{
		URL:     srv.URL,
		Headers: map[string]string{"Authorization": "Bearer t"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if gotUA != "rss2msg/test" {
		t.Fatalf("ua=%q", gotUA)
	}
	if gotAuth != "Bearer t" {
		t.Fatalf("auth=%q", gotAuth)
	}
	if res.Status != 200 || res.Feed == nil || len(res.Feed.Items) != 1 {
		t.Fatalf("bad fetch: %+v", res)
	}
	if res.ETag != `"v1"` || res.LastModified.IsZero() {
		t.Fatalf("cache headers not captured: etag=%q lm=%v", res.ETag, res.LastModified)
	}
}

func TestFetchSendsCacheValidatorHeaders(t *testing.T) {
	t.Parallel()
	var gotInm, gotIms string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotInm = r.Header.Get("If-None-Match")
		gotIms = r.Header.Get("If-Modified-Since")
		w.WriteHeader(304)
	}))
	defer srv.Close()

	f := NewFetcher(Options{UserAgent: "rss2msg/test"})
	when := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	res, err := f.Fetch(context.Background(), FetchRequest{
		URL:          srv.URL,
		ETag:         `"v1"`,
		LastModified: when,
	})
	if err != nil {
		t.Fatal(err)
	}
	if gotInm != `"v1"` {
		t.Fatalf("If-None-Match=%q", gotInm)
	}
	if !strings.Contains(gotIms, "01 May 2026") {
		t.Fatalf("If-Modified-Since=%q", gotIms)
	}
	if res.Status != 304 || res.NotModified != true || res.Feed != nil {
		t.Fatalf("expected 304 with no feed, got %+v", res)
	}
}

func TestFetchEmptyHeaderValueSuppressesDefaultUA(t *testing.T) {
	t.Parallel()
	var gotUA string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotUA = r.Header.Get("User-Agent")
		_, _ = w.Write([]byte(sampleRSS))
	}))
	defer srv.Close()
	f := NewFetcher(Options{UserAgent: "rss2msg/test"})
	_, err := f.Fetch(context.Background(), FetchRequest{
		URL:     srv.URL,
		Headers: map[string]string{"User-Agent": ""},
	})
	if err != nil {
		t.Fatal(err)
	}
	if gotUA != "" {
		t.Fatalf("expected empty UA, got %q", gotUA)
	}
}

func TestFetchHonoursPerRequestTimeout(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Millisecond)
		_, _ = w.Write([]byte(sampleRSS))
	}))
	defer srv.Close()
	f := NewFetcher(Options{UserAgent: "ua", Timeout: 5 * time.Second})
	_, err := f.Fetch(context.Background(), FetchRequest{URL: srv.URL, Timeout: 10 * time.Millisecond})
	if err == nil {
		t.Fatalf("expected timeout error")
	}
}

func TestFetchReturnsTypedStatusError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	_, err := NewFetcher(Options{}).Fetch(context.Background(), FetchRequest{URL: srv.URL})
	var fe *FetchError
	if !errors.As(err, &fe) {
		t.Fatalf("expected *FetchError, got %T: %v", err, err)
	}
	if fe.Op != "status" || fe.Status != http.StatusServiceUnavailable {
		t.Fatalf("got Op=%q Status=%d", fe.Op, fe.Status)
	}
	if !IsRetryable(err) {
		t.Fatalf("503 should be retryable")
	}
}

func TestFetchReturnsTypedParseError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("not xml at all"))
	}))
	defer srv.Close()

	_, err := NewFetcher(Options{}).Fetch(context.Background(), FetchRequest{URL: srv.URL})
	var fe *FetchError
	if !errors.As(err, &fe) {
		t.Fatalf("expected *FetchError, got %T: %v", err, err)
	}
	if fe.Op != "parse" {
		t.Fatalf("got Op=%q, want parse", fe.Op)
	}
	if IsRetryable(err) {
		t.Fatalf("parse errors must not be retryable")
	}
}
