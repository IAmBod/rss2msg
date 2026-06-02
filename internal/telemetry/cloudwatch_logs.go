package telemetry

import (
	"context"
	"errors"
	"sort"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	cwltypes "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/trace"

	"github.com/iambod/rss2msg/internal/config"
)

// maxLogBatch bounds how many events are sent in a single PutLogEvents call.
// CloudWatch caps a batch at 10,000 events (and 1 MB); we chunk on count.
const maxLogBatch = 10000

// cwLogEvent is a single buffered log record awaiting shipment to CloudWatch.
type cwLogEvent struct {
	ts  time.Time
	msg string
}

// cloudWatchLogsAPI is the subset of the CloudWatch Logs client the shipper
// depends on, narrowed so a fake can satisfy it without contacting AWS.
type cloudWatchLogsAPI interface {
	PutLogEvents(context.Context, *cloudwatchlogs.PutLogEventsInput, ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.PutLogEventsOutput, error)
	CreateLogGroup(context.Context, *cloudwatchlogs.CreateLogGroupInput, ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.CreateLogGroupOutput, error)
	CreateLogStream(context.Context, *cloudwatchlogs.CreateLogStreamInput, ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.CreateLogStreamOutput, error)
}

// cloudWatchLogsHook is a zerolog hook that enqueues log events at or above
// minLevel onto a buffered channel for asynchronous batched shipment. Run must
// not block (it runs on the logging path), so a full buffer drops the event and
// increments a counter rather than waiting. When the event carries an OTEL span
// context, trace_id/span_id are appended to the message (mirrors otelLogHook).
//
// zerolog hooks only expose the level and message — not structured fields or the
// err object — so the shipped record carries the message string.
type cloudWatchLogsHook struct {
	minLevel zerolog.Level
	ch       chan cwLogEvent
	dropped  *atomic.Int64
}

func (h cloudWatchLogsHook) Run(e *zerolog.Event, level zerolog.Level, msg string) {
	// NoLevel/Disabled sort numerically above the real levels; never forward
	// them regardless of the configured threshold.
	if level == zerolog.NoLevel || level == zerolog.Disabled || level < h.minLevel {
		return
	}
	m := msg
	if ctx := e.GetCtx(); ctx != nil {
		if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
			m = msg + " trace_id=" + sc.TraceID().String() + " span_id=" + sc.SpanID().String()
		}
	}
	select {
	case h.ch <- cwLogEvent{ts: time.Now(), msg: m}:
	default:
		h.dropped.Add(1)
	}
}

// logsShipper drains the hook's channel in a background goroutine and flushes
// buffered events to CloudWatch Logs on an interval (or when the buffer reaches
// maxBatch). Shipment is best-effort: PutLogEvents errors are reported via
// onError but never surface to the application.
type logsShipper struct {
	api         cloudWatchLogsAPI
	group       string
	stream      string
	ch          chan cwLogEvent
	interval    time.Duration
	maxBatch    int
	createGroup bool
	onError     func(error)
	dropped     *atomic.Int64

	stop         chan struct{}
	done         chan struct{}
	lastReported int64
}

// reportDrops surfaces any newly-dropped events (buffer-full on the logging
// path) via onError so silent loss is at least observable.
func (s *logsShipper) reportDrops() {
	if s.dropped == nil {
		return
	}
	if total := s.dropped.Load(); total > s.lastReported {
		s.handleErr(&droppedLogsError{count: total - s.lastReported})
		s.lastReported = total
	}
}

func (s *logsShipper) handleErr(err error) {
	if err != nil && s.onError != nil {
		s.onError(err)
	}
}

// run is the shipper's background loop. It owns the buffer; the hook only ever
// sends on s.ch, and s.ch is never closed, so the hook can keep sending during
// shutdown without risking a send-on-closed-channel panic.
func (s *logsShipper) run() {
	defer close(s.done)

	if s.createGroup {
		s.handleErr(ensureLogStream(context.Background(), s.api, s.group, s.stream))
	}

	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	var buf []cwLogEvent
	flush := func() {
		s.reportDrops()
		if len(buf) == 0 {
			return
		}
		s.handleErr(putLogEvents(context.Background(), s.api, s.group, s.stream, s.maxBatch, buf))
		buf = buf[:0]
	}

	for {
		select {
		case ev := <-s.ch:
			buf = append(buf, ev)
			if len(buf) >= s.maxBatch {
				flush()
			}
		case <-ticker.C:
			flush()
		case <-s.stop:
			// Drain whatever is buffered, flush once, and exit.
			for {
				select {
				case ev := <-s.ch:
					buf = append(buf, ev)
				default:
					flush()
					return
				}
			}
		}
	}
}

func (s *logsShipper) shutdown(ctx context.Context) error {
	close(s.stop)
	select {
	case <-s.done:
	case <-ctx.Done():
	}
	return nil
}

// putLogEvents ships events to CloudWatch Logs, sorted chronologically (a
// PutLogEvents requirement) and chunked into batches of at most maxBatch.
func putLogEvents(ctx context.Context, api cloudWatchLogsAPI, group, stream string, maxBatch int, events []cwLogEvent) error {
	if len(events) == 0 {
		return nil
	}
	if maxBatch <= 0 {
		maxBatch = maxLogBatch
	}
	sort.SliceStable(events, func(i, j int) bool { return events[i].ts.Before(events[j].ts) })

	for start := 0; start < len(events); start += maxBatch {
		end := start + maxBatch
		if end > len(events) {
			end = len(events)
		}
		batch := make([]cwltypes.InputLogEvent, 0, end-start)
		for _, ev := range events[start:end] {
			batch = append(batch, cwltypes.InputLogEvent{
				Message:   aws.String(ev.msg),
				Timestamp: aws.Int64(ev.ts.UnixMilli()),
			})
		}
		if _, err := api.PutLogEvents(ctx, &cloudwatchlogs.PutLogEventsInput{
			LogGroupName:  aws.String(group),
			LogStreamName: aws.String(stream),
			LogEvents:     batch,
		}); err != nil {
			return err
		}
	}
	return nil
}

// ensureLogStream creates the log group and stream, treating an
// already-exists response as success.
func ensureLogStream(ctx context.Context, api cloudWatchLogsAPI, group, stream string) error {
	if _, err := api.CreateLogGroup(ctx, &cloudwatchlogs.CreateLogGroupInput{
		LogGroupName: aws.String(group),
	}); err != nil && !isResourceAlreadyExists(err) {
		return err
	}
	if _, err := api.CreateLogStream(ctx, &cloudwatchlogs.CreateLogStreamInput{
		LogGroupName:  aws.String(group),
		LogStreamName: aws.String(stream),
	}); err != nil && !isResourceAlreadyExists(err) {
		return err
	}
	return nil
}

func isResourceAlreadyExists(err error) bool {
	var e *cwltypes.ResourceAlreadyExistsException
	return errors.As(err, &e)
}

// setupCloudWatchLogs builds a CloudWatch Logs client from cfg, starts a
// background shipper, and returns a zerolog hook plus a shutdown func that
// flushes the buffer. The returned logger writes shipper errors via onError.
func setupCloudWatchLogs(ctx context.Context, cfg config.TelemetryCloudWatchConfig, onError func(error)) (zerolog.Hook, func(context.Context) error, error) {
	level := cfg.Logs.Level
	if level == "" {
		level = "info"
	}
	minLevel, err := zerolog.ParseLevel(level)
	if err != nil || minLevel == zerolog.NoLevel {
		return nil, nil, errors.New("parse cloudwatch logs level " + cfg.Logs.Level)
	}

	awsCfg, err := loadCloudWatchAWSConfig(ctx, cfg)
	if err != nil {
		return nil, nil, err
	}
	var clientOpts []func(*cloudwatchlogs.Options)
	if cfg.EndpointURL != "" {
		clientOpts = append(clientOpts, func(o *cloudwatchlogs.Options) {
			o.BaseEndpoint = aws.String(cfg.EndpointURL)
		})
	}
	client := cloudwatchlogs.NewFromConfig(awsCfg, clientOpts...)

	interval := cfg.Logs.BatchInterval
	if interval <= 0 {
		interval = 5 * time.Second
	}
	stream := cfg.Logs.LogStream
	if stream == "" {
		stream = hostname()
	}

	ch := make(chan cwLogEvent, 4096)
	dropped := &atomic.Int64{}
	s := &logsShipper{
		api:         client,
		group:       cfg.Logs.LogGroup,
		stream:      stream,
		ch:          ch,
		interval:    interval,
		maxBatch:    maxLogBatch,
		createGroup: cfg.Logs.CreateGroup,
		onError:     onError,
		dropped:     dropped,
		stop:        make(chan struct{}),
		done:        make(chan struct{}),
	}
	go s.run()

	hook := cloudWatchLogsHook{minLevel: minLevel, ch: ch, dropped: dropped}
	return hook, s.shutdown, nil
}

// droppedLogsError reports log events discarded because the shipper's buffer was
// full on the logging path.
type droppedLogsError struct{ count int64 }

func (e *droppedLogsError) Error() string {
	return "cloudwatch logs buffer full: dropped " + strconv.FormatInt(e.count, 10) + " events"
}

// loadCloudWatchAWSConfig loads AWS config for CloudWatch, applying the optional
// region override. Credentials resolve lazily via the default chain, so this
// does not perform network I/O.
func loadCloudWatchAWSConfig(ctx context.Context, cfg config.TelemetryCloudWatchConfig) (aws.Config, error) {
	var loadOpts []func(*awsconfig.LoadOptions) error
	if cfg.Region != "" {
		loadOpts = append(loadOpts, awsconfig.WithRegion(cfg.Region))
	}
	return awsconfig.LoadDefaultConfig(ctx, loadOpts...)
}
