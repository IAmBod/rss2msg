package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/iambod/rss2msg/internal/scheduler"
)

func TestImplicitSubcommandLambda(t *testing.T) {
	t.Parallel()
	// AWS Lambda sets AWS_LAMBDA_RUNTIME_API in its execution environment.
	env := func(k string) string {
		if k == "AWS_LAMBDA_RUNTIME_API" {
			return "127.0.0.1:9001"
		}
		return ""
	}
	if got := implicitSubcommand(env); got != "lambda" {
		t.Fatalf("got %q, want %q", got, "lambda")
	}
}

func TestImplicitSubcommandAzure(t *testing.T) {
	t.Parallel()
	// Azure Functions sets FUNCTIONS_CUSTOMHANDLER_PORT for custom handlers.
	env := func(k string) string {
		if k == "FUNCTIONS_CUSTOMHANDLER_PORT" {
			return "8080"
		}
		return ""
	}
	if got := implicitSubcommand(env); got != "azure-functions" {
		t.Fatalf("got %q, want %q", got, "azure-functions")
	}
}

func TestImplicitSubcommandNone(t *testing.T) {
	t.Parallel()
	// Outside any serverless environment there is no implicit subcommand.
	if got := implicitSubcommand(func(string) string { return "" }); got != "" {
		t.Fatalf("expected empty, got %q", got)
	}
}

func TestImplicitSubcommandLambdaTakesPrecedence(t *testing.T) {
	t.Parallel()
	// If both signature vars are set (unlikely), Lambda wins — its runtime loop
	// must be entered, not an HTTP server bound to the wrong port.
	env := func(string) string { return "set" }
	if got := implicitSubcommand(env); got != "lambda" {
		t.Fatalf("expected lambda precedence, got %q", got)
	}
}

func TestCustomHandlerPollsEveryPipelineOnce(t *testing.T) {
	t.Parallel()
	ps := []scheduler.FeedPipeline{
		&fakePipeline{url: "a"},
		&fakePipeline{url: "b"},
		&fakePipeline{url: "c"},
	}
	h := customHandlerFunc(ps, 2)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/poll", strings.NewReader(`{}`))
	h(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	var res invokeResult
	if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if !res.OK || res.Feeds != 3 || res.Error != "" {
		t.Fatalf("unexpected result: %+v", res)
	}
	for _, p := range ps {
		if got := atomic.LoadInt32(&p.(*fakePipeline).calls); got != 1 {
			t.Fatalf("%s: expected 1 poll, got %d", p.FeedURL(), got)
		}
	}
}

func TestCustomHandlerReportsErrors(t *testing.T) {
	t.Parallel()
	ps := []scheduler.FeedPipeline{
		&fakePipeline{url: "a", err: errors.New("a fail")},
		&fakePipeline{url: "b"},
	}
	h := customHandlerFunc(ps, 0)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/poll", strings.NewReader(`{}`))
	h(rec, req)

	// A failed poll must surface as a 5xx so the Functions host marks the
	// invocation failed and retries per the trigger policy.
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 on failure, got %d", rec.Code)
	}
	var res invokeResult
	if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if res.OK {
		t.Fatal("expected result.OK to be false on failure")
	}
	if !strings.Contains(res.Error, "a fail") {
		t.Fatalf("expected error summary to mention the failure, got %q", res.Error)
	}
	if res.Feeds != 2 {
		t.Fatalf("expected Feeds=2, got %d", res.Feeds)
	}
}
