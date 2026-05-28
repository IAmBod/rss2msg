package main

import (
	"context"
	"fmt"

	"github.com/iambod/rss2msg/internal/config"
	"github.com/iambod/rss2msg/internal/feed"
	"github.com/iambod/rss2msg/internal/retry"
	"github.com/iambod/rss2msg/internal/sink"
	sinkkafka "github.com/iambod/rss2msg/internal/sink/kafka"
	sinkpg "github.com/iambod/rss2msg/internal/sink/postgres"
	sinkrabbitmq "github.com/iambod/rss2msg/internal/sink/rabbitmq"
	sinksns "github.com/iambod/rss2msg/internal/sink/sns"
	sinksqs "github.com/iambod/rss2msg/internal/sink/sqs"
	"github.com/iambod/rss2msg/internal/state"
	statepg "github.com/iambod/rss2msg/internal/state/postgres"
	"github.com/iambod/rss2msg/internal/telemetry"
)

type wired struct {
	store     state.Store
	registry  *sink.Registry
	pipelines []*pipeline
}

func (w *wired) Close() {
	if w.registry != nil {
		_ = w.registry.Close()
	}
	if w.store != nil {
		_ = w.store.Close()
	}
}

func wireAll(ctx context.Context, cfg config.Config, tel *telemetry.Telemetry) (*wired, error) {
	st, err := openStateStore(ctx, cfg.State)
	if err != nil {
		return nil, err
	}
	reg := sink.NewRegistry()
	for _, sc := range cfg.Sinks {
		p, err := buildPublisher(ctx, sc)
		if err != nil {
			_ = reg.Close()
			_ = st.Close()
			return nil, err
		}
		if err := reg.Add(p); err != nil {
			_ = reg.Close()
			_ = st.Close()
			return nil, err
		}
	}

	fetcher := feed.NewFetcher(feed.Options{
		UserAgent: cfg.HTTP.UserAgent,
		Timeout:   cfg.HTTP.Timeout,
	})
	det := feed.NewDetector()
	instr, err := telemetry.NewInstruments(tel.Meter)
	if err != nil {
		_ = reg.Close()
		_ = st.Close()
		return nil, fmt.Errorf("instruments: %w", err)
	}

	w := &wired{store: st, registry: reg}
	for _, fc := range cfg.Feeds {
		names := config.ResolveFeedSinks(fc)
		branches := make([]sinkBranch, 0, len(names))
		for _, name := range names {
			primary, ok := reg.Get(name)
			if !ok {
				w.Close()
				return nil, fmt.Errorf("feed %s: unknown sink %q", fc.URL, name)
			}
			scCfg := findSink(cfg.Sinks, name)
			var dlq sink.Publisher
			if scCfg.DeadLetter != "" {
				dlq, _ = reg.Get(scCfg.DeadLetter)
			}
			wrapped := sink.WithRetry(primary, dlq, retry.Config{
				MaxAttempts: cfg.Retry.MaxAttempts,
				BaseDelay:   cfg.Retry.BaseDelay,
				MaxDelay:    cfg.Retry.MaxDelay,
			})
			branches = append(branches, sinkBranch{name: name, wrapped: wrapped})
		}
		w.pipelines = append(w.pipelines, &pipeline{
			cfg:     fc,
			sinks:   branches,
			fetcher: fetcher,
			detect:  det,
			store:   st,
			log:     tel.Logger,
			tracer:  tel.Tracer,
			instr:   instr,
		})
	}
	return w, nil
}

func findSink(list []config.SinkConfig, name string) config.SinkConfig {
	for _, s := range list {
		if s.Name == name {
			return s
		}
	}
	return config.SinkConfig{}
}

func openStateStore(ctx context.Context, c config.StateConfig) (state.Store, error) {
	switch c.Driver {
	case "postgres":
		return statepg.New(ctx, c.Postgres.DSN)
	default:
		return nil, fmt.Errorf("unsupported state driver %q", c.Driver)
	}
}

func buildPublisher(ctx context.Context, sc config.SinkConfig) (sink.Publisher, error) {
	switch sc.Driver {
	case "postgres":
		return sinkpg.New(ctx, sinkpg.Options{Name: sc.Name, DSN: sc.Postgres.DSN, Table: sc.Postgres.Table})
	case "kafka":
		return sinkkafka.New(sinkkafka.Options{
			Name: sc.Name, Brokers: sc.Kafka.Brokers, Topic: sc.Kafka.Topic,
			Acks: sc.Kafka.Acks, Compression: sc.Kafka.Compression,
		})
	case "rabbitmq":
		return sinkrabbitmq.New(sc.Name), nil
	case "sqs":
		return sinksqs.New(sc.Name), nil
	case "sns":
		return sinksns.New(sc.Name), nil
	default:
		return nil, fmt.Errorf("unsupported sink driver %q", sc.Driver)
	}
}
