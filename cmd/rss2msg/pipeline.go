package main

import (
	"context"
	"time"

	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/iambod/rss2msg/internal/config"
	"github.com/iambod/rss2msg/internal/coord"
	"github.com/iambod/rss2msg/internal/feed"
	"github.com/iambod/rss2msg/internal/model"
	"github.com/iambod/rss2msg/internal/sink"
	"github.com/iambod/rss2msg/internal/state"
	"github.com/iambod/rss2msg/internal/telemetry"
)

// pipeline implements scheduler.FeedPipeline by stringing the per-feed
// fetcher -> detector -> sinks together.
type pipeline struct {
	cfg     config.FeedConfig
	sinks   []sinkBranch
	fetcher *feed.Fetcher
	detect  *feed.Detector
	store   state.Store
	log     zerolog.Logger
	tracer  trace.Tracer
	instr   telemetry.Instruments
	coord   coord.Coordinator
}

type sinkBranch struct {
	name    string
	wrapped *sink.RetryingPublisher
}

func (p *pipeline) FeedURL() string { return p.cfg.URL }

func (p *pipeline) RunOnce(ctx context.Context, feedURL string, at time.Time) ([]model.Change, error) {
	ctx, span := p.tracer.Start(ctx, "feed.poll", trace.WithAttributes(attribute.String("feed_url", feedURL)))
	defer span.End()

	log := p.log.With().Str("feed_url", feedURL).Logger()
	ctx = log.WithContext(ctx)

	release, acquired, err := p.coord.TryAcquire(ctx, feedURL)
	if err != nil {
		p.instr.PollSkipped.Add(ctx, 1, metric.WithAttributes(
			attribute.String("feed_url", feedURL),
			attribute.String("reason", "coord_error"),
		))
		log.Warn().Err(err).Msg("coordinator error; skipping poll")
		return nil, nil
	}
	if !acquired {
		p.instr.PollSkipped.Add(ctx, 1, metric.WithAttributes(
			attribute.String("feed_url", feedURL),
			attribute.String("reason", "not_owner"),
		))
		log.Debug().Msg("another instance owns this poll; skipping")
		return nil, nil
	}
	defer func() { _ = release(ctx) }()

	meta, _, err := p.store.GetFeedMeta(ctx, feedURL)
	if err != nil {
		log.Error().Err(err).Msg("read feed meta")
		return nil, err
	}

	fetchCtx, fetchSpan := p.tracer.Start(ctx, "feed.fetch")
	fetchStart := time.Now()
	res, err := p.fetcher.Fetch(fetchCtx, feed.FetchRequest{
		URL:          feedURL,
		Headers:      p.cfg.HTTP.Headers,
		Timeout:      p.cfg.HTTP.Timeout,
		ETag:         meta.ETag,
		LastModified: meta.LastModified,
	})
	p.instr.FeedFetchDuration.Record(ctx, float64(time.Since(fetchStart).Milliseconds()),
		metric.WithAttributes(attribute.String("feed_url", feedURL)))
	p.instr.FeedFetches.Add(ctx, 1,
		metric.WithAttributes(attribute.String("feed_url", feedURL), attribute.Int("http.status", res.Status)))
	fetchSpan.End()
	if err != nil {
		log.Error().Err(err).Msg("fetch")
		return nil, err
	}
	if res.NotModified {
		log.Debug().Msg("not modified")
		return nil, nil
	}

	if err := p.store.UpsertFeedMeta(ctx, feedURL, state.FeedMeta{ETag: res.ETag, LastModified: res.LastModified}); err != nil {
		log.Warn().Err(err).Msg("persist feed meta")
	}

	detectCtx, detectSpan := p.tracer.Start(ctx, "feed.detect_changes")
	changes, err := p.detect.Detect(detectCtx, feedURL, res.Feed, p.store, at)
	detectSpan.End()
	if err != nil {
		log.Error().Err(err).Msg("detect")
		return nil, err
	}

	for _, c := range changes {
		p.instr.FeedChanges.Add(ctx, 1,
			metric.WithAttributes(attribute.String("feed_url", feedURL), attribute.String("kind", string(c.Kind))))
		allOK := true
		for _, b := range p.sinks {
			start := time.Now()
			sinkCtx, sinkSpan := p.tracer.Start(ctx, "sink.publish",
				trace.WithAttributes(
					attribute.String("sink.name", b.name),
					attribute.String("change.kind", string(c.Kind)),
				))
			r := b.wrapped.Deliver(sinkCtx, c)
			sinkSpan.SetAttributes(attribute.Int("attempts", r.Attempts))
			if r.State != sink.BranchSuccess {
				sinkSpan.RecordError(r.Err)
			}
			sinkSpan.End()
			p.instr.SinkPublishDuration.Record(ctx, float64(time.Since(start).Milliseconds()),
				metric.WithAttributes(attribute.String("sink.name", b.name)))

			switch r.State {
			case sink.BranchSuccess:
				log.Debug().Str("item_id", c.ItemID).Str("sink", b.name).Str("kind", string(c.Kind)).Msg("published")
			case sink.BranchDLQ:
				p.instr.SinkPublishFailures.Add(ctx, 1, metric.WithAttributes(attribute.String("sink.name", b.name)))
				log.Warn().Err(r.Err).Str("item_id", c.ItemID).Str("sink", b.name).Int("attempts", r.Attempts).Msg("captured by DLQ")
			case sink.BranchDropped:
				p.instr.SinkPublishFailures.Add(ctx, 1, metric.WithAttributes(attribute.String("sink.name", b.name)))
				allOK = false
				log.Error().Err(r.Err).Str("item_id", c.ItemID).Str("sink", b.name).Int("attempts", r.Attempts).Msg("dropped")
			}
		}
		if allOK {
			if err := p.store.UpsertItem(ctx, feedURL, c.ItemID, c.ContentHash, c.DetectedAt); err != nil {
				log.Error().Err(err).Str("item_id", c.ItemID).Msg("commit state")
			}
		}
	}
	return changes, nil
}
