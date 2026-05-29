package main

import (
	"context"
	"fmt"

	"github.com/iambod/rss2msg/internal/config"
	"github.com/iambod/rss2msg/internal/coord"
	coordmem "github.com/iambod/rss2msg/internal/coord/memory"
	coordpg "github.com/iambod/rss2msg/internal/coord/postgres"
	coordredis "github.com/iambod/rss2msg/internal/coord/redis"
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
	statesqlite "github.com/iambod/rss2msg/internal/state/sqlite"
	"github.com/iambod/rss2msg/internal/telemetry"
)

type wired struct {
	store     state.Store
	registry  *sink.Registry
	coord     coord.Coordinator
	pipelines []*pipeline
}

func (w *wired) Close() {
	if w.registry != nil {
		_ = w.registry.Close()
	}
	if w.coord != nil {
		_ = w.coord.Close()
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

	cd, err := openCoordinator(ctx, cfg.Coordination, cfg.State, len(cfg.Feeds))
	if err != nil {
		_ = reg.Close()
		_ = st.Close()
		return nil, err
	}

	fetcher := feed.NewFetcher(feed.Options{
		UserAgent: cfg.HTTP.UserAgent,
		Timeout:   cfg.HTTP.Timeout,
	})
	det := feed.NewDetector()
	instr, err := telemetry.NewInstruments(tel.Meter)
	if err != nil {
		_ = cd.Close()
		_ = reg.Close()
		_ = st.Close()
		return nil, fmt.Errorf("instruments: %w", err)
	}

	w := &wired{store: st, registry: reg, coord: cd}
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
			coord:   cd,
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

func openCoordinator(ctx context.Context, cc config.CoordinationConfig, sc config.StateConfig, feedCount int) (coord.Coordinator, error) {
	driver := cc.Driver
	if driver == "" {
		driver = "memory"
	}
	switch driver {
	case "memory":
		return coordmem.New(), nil
	case "postgres":
		dsn := cc.Postgres.DSN
		if dsn == "" {
			dsn = sc.Postgres.DSN
		}
		if dsn == "" {
			return nil, fmt.Errorf("coordination postgres: no dsn (and no state.postgres.dsn fallback)")
		}
		return coordpg.New(ctx, coordpg.Options{
			DSN:      dsn,
			MinConns: feedCount,
			TLS:      coordPGTLSFromConfig(cc.Postgres.TLS),
		})
	case "redis":
		return coordredis.New(ctx, coordredis.Options{
			URL:             cc.Redis.URL,
			LockTTL:         cc.Redis.LockTTL,
			RenewalInterval: cc.Redis.RenewalInterval,
			TLS:             redisTLSFromConfig(cc.Redis.TLS),
		})
	default:
		return nil, fmt.Errorf("unsupported coordination driver %q", driver)
	}
}

// statePGTLSFromConfig returns nil when no TLS field is set so the postgres
// state store leaves pgx's DSN-derived TLS config in place.
func statePGTLSFromConfig(t config.StatePGTLSConfig) *statepg.TLSOptions {
	if t.CAFile == "" && t.CertFile == "" && t.KeyFile == "" &&
		t.ServerName == "" && !t.InsecureSkipVerify {
		return nil
	}
	return &statepg.TLSOptions{
		CAFile:             t.CAFile,
		CertFile:           t.CertFile,
		KeyFile:            t.KeyFile,
		ServerName:         t.ServerName,
		InsecureSkipVerify: t.InsecureSkipVerify,
	}
}

// coordPGTLSFromConfig returns nil when no TLS field is set so the postgres
// coordinator leaves pgx's DSN-derived TLS config in place.
func coordPGTLSFromConfig(t config.CoordinationPGTLSConfig) *coordpg.TLSOptions {
	if t.CAFile == "" && t.CertFile == "" && t.KeyFile == "" &&
		t.ServerName == "" && !t.InsecureSkipVerify {
		return nil
	}
	return &coordpg.TLSOptions{
		CAFile:             t.CAFile,
		CertFile:           t.CertFile,
		KeyFile:            t.KeyFile,
		ServerName:         t.ServerName,
		InsecureSkipVerify: t.InsecureSkipVerify,
	}
}

// redisTLSFromConfig returns nil when no TLS field is set so the redis
// coordinator falls back to the default TLS that redis.ParseURL produces
// for rediss:// (system roots, SNI from URL host).
func redisTLSFromConfig(t config.CoordinationRedisTLSConfig) *coordredis.TLSOptions {
	if t.CAFile == "" && t.CertFile == "" && t.KeyFile == "" &&
		t.ServerName == "" && !t.InsecureSkipVerify {
		return nil
	}
	return &coordredis.TLSOptions{
		CAFile:             t.CAFile,
		CertFile:           t.CertFile,
		KeyFile:            t.KeyFile,
		ServerName:         t.ServerName,
		InsecureSkipVerify: t.InsecureSkipVerify,
	}
}

func openStateStore(ctx context.Context, c config.StateConfig) (state.Store, error) {
	switch c.Driver {
	case "postgres":
		return statepg.New(ctx, statepg.Options{
			DSN: c.Postgres.DSN,
			TLS: statePGTLSFromConfig(c.Postgres.TLS),
		})
	case "sqlite":
		return statesqlite.New(ctx, c.SQLite.Path)
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
		return sinksqs.New(ctx, sinksqs.Options{
			Name:        sc.Name,
			QueueURL:    sc.SQS.QueueURL,
			Region:      sc.SQS.Region,
			EndpointURL: sc.SQS.EndpointURL,
		})
	case "sns":
		return sinksns.New(ctx, sinksns.Options{
			Name:        sc.Name,
			TopicARN:    sc.SNS.TopicARN,
			Region:      sc.SNS.Region,
			EndpointURL: sc.SNS.EndpointURL,
		})
	default:
		return nil, fmt.Errorf("unsupported sink driver %q", sc.Driver)
	}
}
