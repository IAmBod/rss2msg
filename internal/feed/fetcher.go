package feed

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/mmcdole/gofeed"
)

type Options struct {
	UserAgent string
	Timeout   time.Duration
	// HTTPClient lets tests inject. Nil = a default client per Fetch call.
	HTTPClient *http.Client
}

type Fetcher struct {
	opts   Options
	parser *gofeed.Parser
}

func NewFetcher(opts Options) *Fetcher {
	if opts.Timeout <= 0 {
		opts.Timeout = 30 * time.Second
	}
	return &Fetcher{opts: opts, parser: gofeed.NewParser()}
}

type FetchRequest struct {
	URL          string
	Headers      map[string]string // overrides; empty value suppresses default
	Timeout      time.Duration     // overrides Options.Timeout when > 0
	ETag         string
	LastModified time.Time
}

type FetchResult struct {
	Status       int
	NotModified  bool
	ETag         string
	LastModified time.Time
	Feed         *gofeed.Feed
}

func (f *Fetcher) Fetch(ctx context.Context, fr FetchRequest) (FetchResult, error) {
	timeout := f.opts.Timeout
	if fr.Timeout > 0 {
		timeout = fr.Timeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fr.URL, nil)
	if err != nil {
		return FetchResult{}, &FetchError{Op: "request", Err: fmt.Errorf("build request: %w", err)}
	}

	// Defaults.
	if f.opts.UserAgent != "" {
		req.Header.Set("User-Agent", f.opts.UserAgent)
	}
	if fr.ETag != "" {
		req.Header.Set("If-None-Match", fr.ETag)
	}
	if !fr.LastModified.IsZero() {
		req.Header.Set("If-Modified-Since", fr.LastModified.UTC().Format(http.TimeFormat))
	}

	// Overrides: empty value deletes the default. For headers Go's transport
	// auto-populates when absent (notably User-Agent), set an explicit empty
	// slice so the transport does not add its own value.
	for k, v := range fr.Headers {
		if v == "" {
			req.Header[http.CanonicalHeaderKey(k)] = []string{""}
		} else {
			req.Header.Set(k, v)
		}
	}

	client := f.opts.HTTPClient
	if client == nil {
		client = &http.Client{}
	}
	resp, err := client.Do(req)
	if err != nil {
		return FetchResult{}, &FetchError{Op: "transport", Err: fmt.Errorf("http get: %w", err)}
	}
	defer resp.Body.Close()

	res := FetchResult{
		Status: resp.StatusCode,
		ETag:   resp.Header.Get("ETag"),
	}
	if lm := resp.Header.Get("Last-Modified"); lm != "" {
		if t, err := http.ParseTime(lm); err == nil {
			res.LastModified = t.UTC()
		}
	}

	switch {
	case resp.StatusCode == http.StatusNotModified:
		res.NotModified = true
		return res, nil
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		feed, err := f.parser.Parse(resp.Body)
		if err != nil {
			return res, &FetchError{Op: "parse", Err: fmt.Errorf("parse feed: %w", err)}
		}
		res.Feed = feed
		return res, nil
	default:
		return res, &FetchError{Op: "status", Status: resp.StatusCode, Err: fmt.Errorf("unexpected status %d", resp.StatusCode)}
	}
}
