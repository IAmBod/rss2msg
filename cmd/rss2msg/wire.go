package main

import (
	"context"
	"fmt"
	"os"

	"github.com/iambod/rss2msg/internal/config"
	"github.com/iambod/rss2msg/internal/coord"
	coorddynamo "github.com/iambod/rss2msg/internal/coord/dynamodb"
	coordmem "github.com/iambod/rss2msg/internal/coord/memory"
	coordpg "github.com/iambod/rss2msg/internal/coord/postgres"
	coordredis "github.com/iambod/rss2msg/internal/coord/redis"
	"github.com/iambod/rss2msg/internal/feed"
	"github.com/iambod/rss2msg/internal/retry"
	"github.com/iambod/rss2msg/internal/scheduler"
	"github.com/iambod/rss2msg/internal/sink"
	sinkasb "github.com/iambod/rss2msg/internal/sink/azureservicebus"
	compositesink "github.com/iambod/rss2msg/internal/sink/composite"
	sinkcosmos "github.com/iambod/rss2msg/internal/sink/cosmosdb"
	sinkdapr "github.com/iambod/rss2msg/internal/sink/dapr"
	sinkdynamodb "github.com/iambod/rss2msg/internal/sink/dynamodb"
	feedsink "github.com/iambod/rss2msg/internal/sink/feed"
	sinkgcppubsub "github.com/iambod/rss2msg/internal/sink/gcppubsub"
	sinkgrpc "github.com/iambod/rss2msg/internal/sink/grpc"
	sinkhttp "github.com/iambod/rss2msg/internal/sink/http"
	sinkkafka "github.com/iambod/rss2msg/internal/sink/kafka"
	sinkschema "github.com/iambod/rss2msg/internal/sink/kafka/schema"
	sinknats "github.com/iambod/rss2msg/internal/sink/nats"
	sinkpg "github.com/iambod/rss2msg/internal/sink/postgres"
	sinkrabbitmq "github.com/iambod/rss2msg/internal/sink/rabbitmq"
	sinksns "github.com/iambod/rss2msg/internal/sink/sns"
	sinksqs "github.com/iambod/rss2msg/internal/sink/sqs"
	sinkstdout "github.com/iambod/rss2msg/internal/sink/stdout"
	"github.com/iambod/rss2msg/internal/state"
	statedynamodb "github.com/iambod/rss2msg/internal/state/dynamodb"
	statepg "github.com/iambod/rss2msg/internal/state/postgres"
	statesqlite "github.com/iambod/rss2msg/internal/state/sqlite"
	"github.com/iambod/rss2msg/internal/telemetry"
)

type wired struct {
	store    state.Store
	registry *sink.Registry
	coord    coord.Coordinator
	factory  scheduler.PipelineFactory
	instr    telemetry.Instruments
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

// newPipelineFactory returns a factory that builds a *pipeline for any feed,
// sharing the wired fetcher/detector/store/coord/instruments. Used both at boot
// and by ServeDynamic to construct pipelines for feeds added at runtime.
func (w *wired) newPipelineFactory(cfg config.Config, tel *telemetry.Telemetry, fetcher *feed.Fetcher, det *feed.Detector, instr telemetry.Instruments) scheduler.PipelineFactory {
	return func(fc config.FeedConfig) (scheduler.FeedPipeline, error) {
		names := config.ResolveFeedSinks(fc)
		branches := make([]sinkBranch, 0, len(names))
		for _, name := range names {
			wrapped, err := wrapSink(w.registry, cfg, name)
			if err != nil {
				return nil, fmt.Errorf("feed %s: %w", fc.URL, err)
			}
			branches = append(branches, sinkBranch{name: name, wrapped: wrapped})
		}
		return &pipeline{
			cfg:     fc,
			sinks:   branches,
			fetcher: fetcher,
			detect:  det,
			store:   w.store,
			log:     tel.Logger,
			tracer:  tel.Tracer,
			instr:   instr,
			coord:   w.coord,
		}, nil
	}
}

func wireAll(ctx context.Context, cfg config.Config, tel *telemetry.Telemetry) (*wired, error) {
	st, err := openStateStore(ctx, cfg.State)
	if err != nil {
		return nil, err
	}
	reg := sink.NewRegistry()
	for _, sc := range cfg.Sinks {
		p, err := buildPublisher(ctx, sc, tel)
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

	if err := linkComposites(reg, cfg); err != nil {
		_ = reg.Close()
		_ = st.Close()
		return nil, err
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

	w := &wired{store: st, registry: reg, coord: cd, instr: instr}
	// Pipelines are built lazily from the resolved feed set: serve via the
	// aggregator + factory, the one-shot modes via buildOneShotPipelines. Both
	// share this factory, so feed resolution (cfg.Feeds + feed_sources) is
	// consistent across every execution mode.
	w.factory = w.newPipelineFactory(cfg, tel, fetcher, det, instr)
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

// wrapSink resolves a sink by name and wraps it for delivery. A composite owns
// its per-child retry/DLQ internally and must never be retried as a unit (that
// would re-send to children that already succeeded), so it is wrapped
// pass-through: one attempt, no DLQ. Every other driver gets the global retry
// budget plus its configured dead_letter.
func wrapSink(reg *sink.Registry, cfg config.Config, name string) (*sink.RetryingPublisher, error) {
	primary, ok := reg.Get(name)
	if !ok {
		return nil, fmt.Errorf("unknown sink %q", name)
	}
	scCfg := findSink(cfg.Sinks, name)
	if scCfg.Driver == "composite" {
		return sink.WithRetry(primary, nil, retry.Config{MaxAttempts: 1}, cfg.Runtime.DeliverTimeout), nil
	}
	var dlq sink.Publisher
	if scCfg.DeadLetter != "" {
		dlq, _ = reg.Get(scCfg.DeadLetter)
	}
	return sink.WithRetry(primary, dlq, retry.Config{
		MaxAttempts: cfg.Retry.MaxAttempts,
		BaseDelay:   cfg.Retry.BaseDelay,
		MaxDelay:    cfg.Retry.MaxDelay,
	}, cfg.Runtime.DeliverTimeout), nil
}

// linkComposites resolves each composite sink's children from the registry and
// attaches the wrapped branches. Called after every sink is built and added,
// so child pointers (including nested composites) are already present.
func linkComposites(reg *sink.Registry, cfg config.Config) error {
	for _, sc := range cfg.Sinks {
		if sc.Driver != "composite" {
			continue
		}
		p, ok := reg.Get(sc.Name)
		if !ok {
			return fmt.Errorf("composite %q: not in registry (bug)", sc.Name)
		}
		comp, ok := p.(*compositesink.Publisher)
		if !ok {
			return fmt.Errorf("composite %q: expected *compositesink.Publisher, got %T", sc.Name, p)
		}
		branches := make([]compositesink.Branch, 0, len(sc.Composite.Children))
		for _, child := range sc.Composite.Children {
			wrapped, err := wrapSink(reg, cfg, child)
			if err != nil {
				return fmt.Errorf("composite %q: child %q: %w", sc.Name, child, err)
			}
			branches = append(branches, compositesink.Branch{Name: child, Wrapped: wrapped})
		}
		comp.SetBranches(branches)
	}
	return nil
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
		return coordredis.New(ctx, redisCoordOptions(cc))
	case "dynamodb":
		return coorddynamo.New(ctx, coorddynamo.Options{
			Table:         cc.DynamoDB.Table,
			Region:        cc.DynamoDB.Region,
			EndpointURL:   cc.DynamoDB.EndpointURL,
			LeaseDuration: cc.DynamoDB.LeaseDuration,
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

// sinkPGTLSFromConfig maps the canonical sink TLS block to the postgres sink's
// TLS options, returning nil when the block is inactive.
func sinkPGTLSFromConfig(t config.SinkTLSConfig) *sinkpg.TLSOptions {
	if !t.Active() {
		return nil
	}
	return &sinkpg.TLSOptions{
		CAFile:             t.CAFile,
		CertFile:           t.CertFile,
		KeyFile:            t.KeyFile,
		ServerName:         t.ServerName,
		InsecureSkipVerify: t.InsecureSkipVerify,
	}
}

// sinkKafkaTLSFromConfig maps the canonical sink TLS block to the kafka sink's
// TLS options, returning nil when the block is inactive.
func sinkKafkaTLSFromConfig(t config.SinkTLSConfig) *sinkkafka.TLSOptions {
	if !t.Active() {
		return nil
	}
	return &sinkkafka.TLSOptions{
		CAFile:             t.CAFile,
		CertFile:           t.CertFile,
		KeyFile:            t.KeyFile,
		ServerName:         t.ServerName,
		InsecureSkipVerify: t.InsecureSkipVerify,
	}
}

// schemaOptionsFromConfig maps the kafka schema_registry config to the sink's
// schema.Options. Returns nil when the registry is not configured (url empty),
// which keeps the plain-JSON value path. auto_register defaults to true when
// unset.
func schemaOptionsFromConfig(topic string, c config.SchemaRegistryConfig) (*sinkschema.Options, error) {
	if c.URL == "" {
		return nil, nil
	}
	o := &sinkschema.Options{
		URL:          c.URL,
		Format:       sinkschema.Format(c.Format),
		Topic:        topic,
		Subject:      c.Subject,
		AutoRegister: c.AutoRegister == nil || *c.AutoRegister,
		BasicUser:    c.BasicAuth.Username,
		BasicPass:    c.BasicAuth.Password,
		TLS:          schemaRegistryTLSFromConfig(c.TLS),
	}
	if c.SchemaFile != "" {
		b, err := os.ReadFile(c.SchemaFile)
		if err != nil {
			return nil, fmt.Errorf("schema_registry.schema_file: %w", err)
		}
		o.SchemaText = string(b)
	}
	return o, nil
}

// schemaRegistryTLSFromConfig maps the canonical sink TLS block to the schema
// registry client's TLS options, returning nil when the block is inactive.
func schemaRegistryTLSFromConfig(t config.SinkTLSConfig) *sinkschema.TLSOptions {
	if !t.Active() {
		return nil
	}
	return &sinkschema.TLSOptions{
		CAFile:             t.CAFile,
		CertFile:           t.CertFile,
		KeyFile:            t.KeyFile,
		ServerName:         t.ServerName,
		InsecureSkipVerify: t.InsecureSkipVerify,
	}
}

// sinkRabbitMQTLSFromConfig maps the canonical sink TLS block to the rabbitmq
// sink's TLS options, returning nil when the block is inactive.
func sinkRabbitMQTLSFromConfig(t config.SinkTLSConfig) *sinkrabbitmq.TLSOptions {
	if !t.Active() {
		return nil
	}
	return &sinkrabbitmq.TLSOptions{
		CAFile:             t.CAFile,
		CertFile:           t.CertFile,
		KeyFile:            t.KeyFile,
		ServerName:         t.ServerName,
		InsecureSkipVerify: t.InsecureSkipVerify,
	}
}

// sinkNATSTLSFromConfig maps the canonical sink TLS block to the nats sink's
// TLS options, returning nil when the block is inactive.
func sinkNATSTLSFromConfig(t config.SinkTLSConfig) *sinknats.TLSOptions {
	if !t.Active() {
		return nil
	}
	return &sinknats.TLSOptions{
		CAFile:             t.CAFile,
		CertFile:           t.CertFile,
		KeyFile:            t.KeyFile,
		ServerName:         t.ServerName,
		InsecureSkipVerify: t.InsecureSkipVerify,
	}
}

// sinkHTTPTLSFromConfig maps the canonical sink TLS block to the http sink's
// TLS options, returning nil when the block is inactive.
func sinkHTTPTLSFromConfig(t config.SinkTLSConfig) *sinkhttp.TLSOptions {
	if !t.Active() {
		return nil
	}
	return &sinkhttp.TLSOptions{
		CAFile:             t.CAFile,
		CertFile:           t.CertFile,
		KeyFile:            t.KeyFile,
		ServerName:         t.ServerName,
		InsecureSkipVerify: t.InsecureSkipVerify,
	}
}

// sinkGRPCTLSFromConfig maps the canonical sink TLS block to the grpc sink's
// TLS options, returning nil when the block is inactive (insecure transport).
func sinkGRPCTLSFromConfig(t config.SinkTLSConfig) *sinkgrpc.TLSOptions {
	if !t.Active() {
		return nil
	}
	return &sinkgrpc.TLSOptions{
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

// redisCoordOptions maps coordination config into coordredis.Options for all modes.
func redisCoordOptions(cc config.CoordinationConfig) coordredis.Options {
	r := cc.Redis
	return coordredis.Options{
		Mode:            r.Mode,
		URL:             r.URL,
		LockTTL:         r.LockTTL,
		RenewalInterval: r.RenewalInterval,
		TLS:             redisTLSFromConfig(r.TLS),
		Sentinel: coordredis.SentinelOptions{
			MasterName:       r.Sentinel.MasterName,
			Addrs:            r.Sentinel.Addrs,
			Username:         r.Sentinel.Username,
			Password:         r.Sentinel.Password,
			SentinelUsername: r.Sentinel.SentinelUsername,
			SentinelPassword: r.Sentinel.SentinelPassword,
			DB:               r.Sentinel.DB,
		},
		Cluster: coordredis.ClusterOptions{
			Addrs:    r.Cluster.Addrs,
			Username: r.Cluster.Username,
			Password: r.Cluster.Password,
		},
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
	case "dynamodb":
		return statedynamodb.New(ctx, statedynamodb.Options{
			Table:        c.DynamoDB.Table,
			Region:       c.DynamoDB.Region,
			EndpointURL:  c.DynamoDB.EndpointURL,
			TTLAttribute: c.DynamoDB.TTLAttribute,
			ItemTTL:      c.DynamoDB.ItemTTL,
		})
	default:
		return nil, fmt.Errorf("unsupported state driver %q", c.Driver)
	}
}

func buildPublisher(ctx context.Context, sc config.SinkConfig, tel *telemetry.Telemetry) (sink.Publisher, error) {
	switch sc.Driver {
	case "postgres":
		return sinkpg.New(ctx, sinkpg.Options{
			Name: sc.Name, DSN: sc.Postgres.DSN, Table: sc.Postgres.Table,
			TLS: sinkPGTLSFromConfig(sc.Postgres.TLS),
		})
	case "kafka":
		schemaOpts, err := schemaOptionsFromConfig(sc.Kafka.Topic, sc.Kafka.SchemaRegistry)
		if err != nil {
			return nil, fmt.Errorf("kafka sink %q: %w", sc.Name, err)
		}
		return sinkkafka.New(sinkkafka.Options{
			Name: sc.Name, Brokers: sc.Kafka.Brokers, Topic: sc.Kafka.Topic,
			Acks: sc.Kafka.Acks, Compression: sc.Kafka.Compression,
			TLS:    sinkKafkaTLSFromConfig(sc.Kafka.TLS),
			Schema: schemaOpts,
		})
	case "stdout":
		return sinkstdout.New(sinkstdout.Options{
			Name:   sc.Name,
			Target: sc.Stdout.Target,
			Format: sc.Stdout.Format,
		})
	case "http":
		return sinkhttp.New(sinkhttp.Options{
			Name:         sc.Name,
			URL:          sc.HTTP.URL,
			Method:       sc.HTTP.Method,
			Headers:      sc.HTTP.Headers,
			Timeout:      sc.HTTP.Timeout,
			SuccessCodes: sc.HTTP.SuccessCodes,
			TLS:          sinkHTTPTLSFromConfig(sc.HTTP.TLS),
			HTTP3:        sc.HTTP.HTTP3,
		})
	case "grpc":
		return sinkgrpc.New(sinkgrpc.Options{
			Name:      sc.Name,
			Target:    sc.GRPC.Target,
			Authority: sc.GRPC.Authority,
			Timeout:   sc.GRPC.Timeout,
			Metadata:  sc.GRPC.Metadata,
			TLS:       sinkGRPCTLSFromConfig(sc.GRPC.TLS),
		})
	case "rabbitmq":
		return sinkrabbitmq.New(sinkrabbitmq.Options{
			Name:         sc.Name,
			URL:          sc.RabbitMQ.URL,
			Exchange:     sc.RabbitMQ.Exchange,
			ExchangeType: sc.RabbitMQ.ExchangeType,
			RoutingKey:   sc.RabbitMQ.RoutingKey,
			Declare:      sc.RabbitMQ.Declare,
			Durable:      sc.RabbitMQ.Durable,
			Mandatory:    sc.RabbitMQ.Mandatory,
			TLS:          sinkRabbitMQTLSFromConfig(sc.RabbitMQ.TLS),
		})
	case "sqs":
		return sinksqs.New(ctx, sinksqs.Options{
			Name:         sc.Name,
			QueueURL:     sc.SQS.QueueURL,
			Region:       sc.SQS.Region,
			EndpointURL:  sc.SQS.EndpointURL,
			MessageGroup: sc.SQS.MessageGroup,
		})
	case "sns":
		return sinksns.New(ctx, sinksns.Options{
			Name:         sc.Name,
			TopicARN:     sc.SNS.TopicARN,
			Region:       sc.SNS.Region,
			EndpointURL:  sc.SNS.EndpointURL,
			MessageGroup: sc.SNS.MessageGroup,
		})
	case "dynamodb":
		return sinkdynamodb.New(ctx, sinkdynamodb.Options{
			Name:         sc.Name,
			Table:        sc.DynamoDB.Table,
			Region:       sc.DynamoDB.Region,
			EndpointURL:  sc.DynamoDB.EndpointURL,
			PartitionKey: sc.DynamoDB.PartitionKey,
			SortKey:      sc.DynamoDB.SortKey,
			TTLAttribute: sc.DynamoDB.TTLAttribute,
			ItemTTL:      sc.DynamoDB.ItemTTL,
		})
	case "feed":
		f := sc.Feed
		var pgTLS *feedsink.PGTLSOptions
		t := f.Store.Postgres.TLS
		if t.CAFile != "" || t.CertFile != "" || t.KeyFile != "" || t.ServerName != "" || t.InsecureSkipVerify {
			pgTLS = &feedsink.PGTLSOptions{
				CAFile: t.CAFile, CertFile: t.CertFile, KeyFile: t.KeyFile,
				ServerName: t.ServerName, InsecureSkipVerify: t.InsecureSkipVerify,
			}
		}
		var auth *feedsink.AuthConfig
		if f.Auth.Basic.Username != "" || f.Auth.BearerToken != "" {
			auth = &feedsink.AuthConfig{
				BasicUser: f.Auth.Basic.Username, BasicPass: f.Auth.Basic.Password,
				BearerToken: f.Auth.BearerToken,
			}
		}
		title := f.Title
		if title == "" {
			title = sc.Name
		}
		max := f.MaxItems
		if max == 0 {
			max = 50
		}
		return feedsink.New(ctx, feedsink.Options{
			Name: sc.Name, Listen: f.Listen, PublicURL: f.PublicURL,
			Meta:           feedsink.FeedMeta{Title: title, Link: f.Link, Description: f.Description},
			MaxItems:       max,
			RSS:            feedsink.Surface{Enabled: f.RSS.On(true), Path: f.RSS.PathOr("/rss")},
			Atom:           feedsink.Surface{Enabled: f.Atom.On(true), Path: f.Atom.PathOr("/atom")},
			MCP:            feedsink.Surface{Enabled: f.MCP.On(false), Path: f.MCP.PathOr("/mcp")},
			RenderCacheTTL: f.RenderCacheTTL, CacheControlTTL: f.CacheControlTTL,
			Timeouts: feedsink.Timeouts{
				ReadHeader: f.Timeouts.ReadHeader, Read: f.Timeouts.Read,
				Write: f.Timeouts.Write, Idle: f.Timeouts.Idle, Shutdown: f.Timeouts.Shutdown,
			},
			TLSCertFile: f.TLS.CertFile, TLSKeyFile: f.TLS.KeyFile, HTTP3: f.HTTP3, Auth: auth,
			StoreDriver: f.Store.Driver, SQLitePath: f.Store.SQLite.Path,
			PostgresDSN: f.Store.Postgres.DSN, Table: f.Store.Postgres.Table, PostgresTLS: pgTLS,
			Meter: tel.Meter, Logger: tel.Logger,
		})
	case "gcp_pubsub":
		return sinkgcppubsub.New(ctx, sinkgcppubsub.Options{
			Name:        sc.Name,
			ProjectID:   sc.GCPPubSub.ProjectID,
			TopicID:     sc.GCPPubSub.TopicID,
			Endpoint:    sc.GCPPubSub.Endpoint,
			OrderingKey: sc.GCPPubSub.OrderingKey,
		})
	case "dapr_pubsub":
		return sinkdapr.New(ctx, sinkdapr.Options{
			Name:        sc.Name,
			Address:     sc.DaprPubSub.Address,
			PubsubName:  sc.DaprPubSub.PubsubName,
			Topic:       sc.DaprPubSub.Topic,
			ContentType: sc.DaprPubSub.ContentType,
			Metadata:    sc.DaprPubSub.Metadata,
		})
	case "nats":
		return sinknats.New(sinknats.Options{
			Name:      sc.Name,
			URL:       sc.NATS.URL,
			Subject:   sc.NATS.Subject,
			Token:     sc.NATS.Token,
			Username:  sc.NATS.Username,
			Password:  sc.NATS.Password,
			CredsFile: sc.NATS.CredsFile,
			JetStream: sc.NATS.JetStream,
			TLS:       sinkNATSTLSFromConfig(sc.NATS.TLS),
		})
	case "azureservicebus":
		return sinkasb.New(sinkasb.Options{
			Name:             sc.Name,
			ConnectionString: sc.AzureServiceBus.ConnectionString,
			Namespace:        sc.AzureServiceBus.Namespace,
			Queue:            sc.AzureServiceBus.Queue,
			Topic:            sc.AzureServiceBus.Topic,
		})
	case "cosmosdb":
		return sinkcosmos.New(ctx, sinkcosmos.Options{
			Name:             sc.Name,
			Endpoint:         sc.CosmosDB.Endpoint,
			ConnectionString: sc.CosmosDB.ConnectionString,
			Database:         sc.CosmosDB.Database,
			Container:        sc.CosmosDB.Container,
			CreateIfMissing:  sc.CosmosDB.CreateIfMissing,
			Throughput:       sc.CosmosDB.Throughput,
		})
	case "composite":
		return compositesink.New(compositesink.Options{
			Name:     sc.Name,
			Children: sc.Composite.Children,
			Logger:   tel.Logger,
			Meter:    tel.Meter,
		})
	default:
		return nil, fmt.Errorf("unsupported sink driver %q", sc.Driver)
	}
}
