package telemetry

import (
	"bytes"
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	cwltypes "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/rs/zerolog"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/iambod/rss2msg/internal/config"
)

// fakeCWLogs records CloudWatch Logs API calls in memory so tests can assert
// what the shipper produced without contacting AWS.
type fakeCWLogs struct {
	mu        sync.Mutex
	puts      [][]cwltypes.InputLogEvent
	groups    []string
	streams   []string
	putErr    error
	createErr error
}

func (f *fakeCWLogs) PutLogEvents(_ context.Context, in *cloudwatchlogs.PutLogEventsInput, _ ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.PutLogEventsOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.putErr != nil {
		return nil, f.putErr
	}
	f.puts = append(f.puts, in.LogEvents)
	return &cloudwatchlogs.PutLogEventsOutput{}, nil
}

func (f *fakeCWLogs) CreateLogGroup(_ context.Context, in *cloudwatchlogs.CreateLogGroupInput, _ ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.CreateLogGroupOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.createErr != nil {
		return nil, f.createErr
	}
	f.groups = append(f.groups, *in.LogGroupName)
	return &cloudwatchlogs.CreateLogGroupOutput{}, nil
}

func (f *fakeCWLogs) CreateLogStream(_ context.Context, in *cloudwatchlogs.CreateLogStreamInput, _ ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.CreateLogStreamOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.createErr != nil {
		return nil, f.createErr
	}
	f.streams = append(f.streams, *in.LogStreamName)
	return &cloudwatchlogs.CreateLogStreamOutput{}, nil
}

func TestCloudWatchLogsHookEnqueuesAtOrAboveLevel(t *testing.T) {
	t.Parallel()
	ch := make(chan cwLogEvent, 8)
	var dropped atomic.Int64
	hook := cloudWatchLogsHook{minLevel: zerolog.WarnLevel, ch: ch, dropped: &dropped}

	logger := zerolog.New(&bytes.Buffer{}).Hook(hook)
	logger.Info().Msg("ignored")
	logger.Warn().Msg("kept")
	logger.Error().Msg("also kept")

	got := drain(ch)
	if len(got) != 2 {
		t.Fatalf("expected 2 enqueued events, got %d (%v)", len(got), got)
	}
	if got[0].msg != "kept" || got[1].msg != "also kept" {
		t.Fatalf("unexpected messages: %v", got)
	}
}

func TestCloudWatchLogsHookCountsDropsWhenBufferFull(t *testing.T) {
	t.Parallel()
	ch := make(chan cwLogEvent, 1)
	var dropped atomic.Int64
	hook := cloudWatchLogsHook{minLevel: zerolog.InfoLevel, ch: ch, dropped: &dropped}

	logger := zerolog.New(&bytes.Buffer{}).Hook(hook)
	logger.Info().Msg("first")  // fills the buffer
	logger.Info().Msg("second") // dropped
	logger.Info().Msg("third")  // dropped

	if d := dropped.Load(); d != 2 {
		t.Fatalf("expected 2 drops, got %d", d)
	}
}

func TestCloudWatchLogsHookAttachesTraceID(t *testing.T) {
	t.Parallel()
	ch := make(chan cwLogEvent, 4)
	var dropped atomic.Int64
	hook := cloudWatchLogsHook{minLevel: zerolog.InfoLevel, ch: ch, dropped: &dropped}

	tp := sdktrace.NewTracerProvider()
	defer func() { _ = tp.Shutdown(context.Background()) }()
	ctx, span := tp.Tracer("test").Start(context.Background(), "s")
	defer span.End()

	logger := zerolog.New(&bytes.Buffer{}).Hook(hook)
	logger.Info().Ctx(ctx).Msg("traced")

	got := drain(ch)
	if len(got) != 1 {
		t.Fatalf("expected 1 event, got %d", len(got))
	}
	want := span.SpanContext().TraceID().String()
	if !bytes.Contains([]byte(got[0].msg), []byte("trace_id="+want)) {
		t.Fatalf("expected trace_id %q in message, got %q", want, got[0].msg)
	}
}

func TestPutLogEventsSortsByTimestamp(t *testing.T) {
	t.Parallel()
	f := &fakeCWLogs{}
	base := time.Unix(1700000000, 0)
	events := []cwLogEvent{
		{ts: base.Add(2 * time.Second), msg: "second"},
		{ts: base, msg: "first"},
		{ts: base.Add(time.Second), msg: "middle"},
	}
	if err := putLogEvents(context.Background(), f, "g", "s", 10, events); err != nil {
		t.Fatalf("putLogEvents: %v", err)
	}
	if len(f.puts) != 1 {
		t.Fatalf("expected 1 PutLogEvents call, got %d", len(f.puts))
	}
	got := f.puts[0]
	if len(got) != 3 || *got[0].Message != "first" || *got[1].Message != "middle" || *got[2].Message != "second" {
		t.Fatalf("events not sorted chronologically: %v", got)
	}
}

func TestPutLogEventsChunksAtBatchLimit(t *testing.T) {
	t.Parallel()
	f := &fakeCWLogs{}
	base := time.Unix(1700000000, 0)
	var events []cwLogEvent
	for i := 0; i < 5; i++ {
		events = append(events, cwLogEvent{ts: base.Add(time.Duration(i) * time.Second), msg: "m"})
	}
	if err := putLogEvents(context.Background(), f, "g", "s", 2, events); err != nil {
		t.Fatalf("putLogEvents: %v", err)
	}
	if len(f.puts) != 3 { // 2 + 2 + 1
		t.Fatalf("expected 3 chunked calls, got %d", len(f.puts))
	}
}

func TestEnsureLogStreamIgnoresAlreadyExists(t *testing.T) {
	t.Parallel()
	f := &fakeCWLogs{createErr: &cwltypes.ResourceAlreadyExistsException{}}
	if err := ensureLogStream(context.Background(), f, "g", "s"); err != nil {
		t.Fatalf("expected ResourceAlreadyExistsException to be ignored, got %v", err)
	}
}

func TestSetupWiresCloudWatchLogsWithoutNetwork(t *testing.T) {
	t.Parallel()
	cfg := config.Defaults()
	cfg.Telemetry.CloudWatch.Enabled = true
	cfg.Telemetry.CloudWatch.Region = "us-east-1"
	cfg.Telemetry.CloudWatch.Logs.Enabled = true
	cfg.Telemetry.CloudWatch.Logs.LogGroup = "/rss2msg/test"
	// CreateGroup stays false, so the background shipper makes no AWS calls
	// with an empty buffer — Setup and Shutdown must not touch the network.

	tel, err := Setup(context.Background(), cfg, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := tel.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

func drain(ch chan cwLogEvent) []cwLogEvent {
	var out []cwLogEvent
	for {
		select {
		case ev := <-ch:
			out = append(out, ev)
		default:
			return out
		}
	}
}
