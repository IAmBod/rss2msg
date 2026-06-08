package config

import "time"

type Config struct {
	Log          LogConfig          `mapstructure:"log"`
	Telemetry    TelemetryConfig    `mapstructure:"telemetry"`
	HTTP         HTTPConfig         `mapstructure:"http"`
	Retry        RetryConfig        `mapstructure:"retry"`
	Runtime      RuntimeConfig      `mapstructure:"runtime"`
	Coordination CoordinationConfig `mapstructure:"coordination"`
	State        StateConfig        `mapstructure:"state"`
	Health       HealthConfig       `mapstructure:"health"`
	Sinks        []SinkConfig       `mapstructure:"sinks"`
	Feeds        []FeedConfig       `mapstructure:"feeds"`
	FeedSources  []FeedSourceConfig `mapstructure:"feed_sources"`
}

type LogConfig struct {
	Level  string `mapstructure:"level"`
	Format string `mapstructure:"format"` // json|console
}

type TelemetryConfig struct {
	ServiceName string                    `mapstructure:"service_name"`
	InstanceID  string                    `mapstructure:"instance_id"` // service.instance.id; defaults to OTEL_SERVICE_INSTANCE_ID then hostname
	Traces      TelemetrySignalConfig     `mapstructure:"traces"`
	Metrics     TelemetrySignalConfig     `mapstructure:"metrics"`
	Logs        TelemetrySignalConfig     `mapstructure:"logs"`
	Prometheus  TelemetryPrometheusConfig `mapstructure:"prometheus"`
	Graphite    TelemetryGraphiteConfig   `mapstructure:"graphite"`
	Sentry      TelemetrySentryConfig     `mapstructure:"sentry"`
	PostHog     TelemetryPostHogConfig    `mapstructure:"posthog"`
	CloudWatch  TelemetryCloudWatchConfig `mapstructure:"cloudwatch"`
}

// TelemetryCloudWatchConfig configures optional AWS CloudWatch telemetry. It is
// disabled by default. Region and EndpointURL are shared by both surfaces; AWS
// credentials are resolved via the default SDK chain (checked at telemetry.Setup,
// not validation). Logs and Metrics toggle independently, so logs-only or
// metrics-only deployments work.
type TelemetryCloudWatchConfig struct {
	Enabled     bool                    `mapstructure:"enabled"`      // master switch for the block
	Region      string                  `mapstructure:"region"`       // AWS region; empty uses the SDK default chain
	EndpointURL string                  `mapstructure:"endpoint_url"` // optional endpoint override (e.g. LocalStack)
	Logs        CloudWatchLogsConfig    `mapstructure:"logs"`
	Metrics     CloudWatchMetricsConfig `mapstructure:"metrics"`
}

// CloudWatchLogsConfig configures the CloudWatch Logs zerolog hook. When Enabled,
// log events at or above Level are batched and shipped to LogGroup/LogStream via
// PutLogEvents.
type CloudWatchLogsConfig struct {
	Enabled       bool          `mapstructure:"enabled"`
	LogGroup      string        `mapstructure:"log_group"`      // required when logs enabled
	LogStream     string        `mapstructure:"log_stream"`     // defaults to the hostname
	Level         string        `mapstructure:"level"`          // min zerolog level forwarded (default "info")
	BatchInterval time.Duration `mapstructure:"batch_interval"` // flush cadence (default 5s)
	CreateGroup   bool          `mapstructure:"create_group"`   // auto-create the group/stream if missing
}

// CloudWatchMetricsConfig configures the CloudWatch Metrics OTEL exporter. When
// Enabled, metrics are pushed to Namespace via PutMetricData on the configured
// Interval through an OTEL PeriodicReader.
type CloudWatchMetricsConfig struct {
	Enabled   bool          `mapstructure:"enabled"`
	Namespace string        `mapstructure:"namespace"` // CloudWatch namespace (default "rss2msg")
	Interval  time.Duration `mapstructure:"interval"`  // push cadence (default 60s)
}

// TelemetryPostHogConfig configures optional PostHog telemetry. It is disabled
// by default; when Enabled, a project API key must be resolvable from APIKey or
// the POSTHOG_API_KEY environment variable (checked at telemetry.Setup, not
// validation). Log events at or above Level are forwarded to PostHog: error and
// above as $exception (Error Tracking) events, lower levels as "log" capture
// events.
type TelemetryPostHogConfig struct {
	Enabled       bool          `mapstructure:"enabled"`
	APIKey        string        `mapstructure:"api_key"`        // project API key; falls back to POSTHOG_API_KEY
	Endpoint      string        `mapstructure:"endpoint"`       // ingestion host; falls back to POSTHOG_ENDPOINT
	DistinctID    string        `mapstructure:"distinct_id"`    // distinct id attached to events; defaults to service name/host
	Level         string        `mapstructure:"level"`          // min zerolog level forwarded (default "error")
	FlushInterval time.Duration `mapstructure:"flush_interval"` // batch flush cadence; 0 uses the SDK default
}

// TelemetrySentryConfig configures optional Sentry error/crash reporting. It is
// disabled by default; when Enabled, a DSN must be resolvable from DSN or the
// SENTRY_DSN environment variable (checked at telemetry.Setup, not validation).
type TelemetrySentryConfig struct {
	Enabled          bool    `mapstructure:"enabled"`
	DSN              string  `mapstructure:"dsn"`                // falls back to SENTRY_DSN
	Environment      string  `mapstructure:"environment"`        // falls back to SENTRY_ENVIRONMENT
	Release          string  `mapstructure:"release"`            // falls back to SENTRY_RELEASE
	ServerName       string  `mapstructure:"server_name"`        // optional host/instance label
	Level            string  `mapstructure:"level"`              // min zerolog level forwarded (default "error")
	SampleRate       float64 `mapstructure:"sample_rate"`        // error-event sampling, [0.0, 1.0]
	TracesSampleRate float64 `mapstructure:"traces_sample_rate"` // performance sampling, [0.0, 1.0]
	Debug            bool    `mapstructure:"debug"`              // Sentry SDK debug logging
}

type TelemetrySignalConfig struct {
	Enabled bool `mapstructure:"enabled"`
}

type TelemetryPrometheusConfig struct {
	Enabled bool   `mapstructure:"enabled"`
	Listen  string `mapstructure:"listen"`
}

// TelemetryGraphiteConfig configures the native Carbon (Graphite plaintext)
// metric exporter. When Enabled, metrics are pushed to Address on the configured
// Interval via an OTEL PeriodicReader.
type TelemetryGraphiteConfig struct {
	Enabled  bool          `mapstructure:"enabled"`
	Address  string        `mapstructure:"address"`  // Carbon plaintext TCP endpoint (host:port)
	Prefix   string        `mapstructure:"prefix"`   // metric-path prefix prepended to every metric
	Interval time.Duration `mapstructure:"interval"` // push cadence; 0 uses the SDK default
}

type HTTPConfig struct {
	UserAgent string        `mapstructure:"user_agent"`
	Timeout   time.Duration `mapstructure:"timeout"`
}

type RetryConfig struct {
	MaxAttempts int           `mapstructure:"max_attempts"`
	BaseDelay   time.Duration `mapstructure:"base_delay"`
	MaxDelay    time.Duration `mapstructure:"max_delay"`
}

type RuntimeConfig struct {
	ShutdownDrainTimeout time.Duration `mapstructure:"shutdown_drain_timeout"`
	RunOnceConcurrency   int           `mapstructure:"run_once_concurrency"`
	// DeliverTimeout bounds a single sink delivery (all retry attempts plus the
	// DLQ handoff) so one wedged sink can't stall a feed's poll loop. 0 = off.
	DeliverTimeout time.Duration `mapstructure:"deliver_timeout"`
}

type CoordinationConfig struct {
	Driver   string                  `mapstructure:"driver"`
	Postgres CoordinationPGConfig    `mapstructure:"postgres"`
	Redis    CoordinationRedisConfig `mapstructure:"redis"`
}

type CoordinationPGConfig struct {
	DSN string                  `mapstructure:"dsn"`
	TLS CoordinationPGTLSConfig `mapstructure:"tls"`
}

type CoordinationPGTLSConfig struct {
	CAFile             string `mapstructure:"ca_file"`
	CertFile           string `mapstructure:"cert_file"`
	KeyFile            string `mapstructure:"key_file"`
	ServerName         string `mapstructure:"server_name"`
	InsecureSkipVerify bool   `mapstructure:"insecure_skip_verify"`
}

type CoordinationRedisConfig struct {
	Mode            string                          `mapstructure:"mode"` // single|sentinel|cluster (default single)
	URL             string                          `mapstructure:"url"`  // single mode
	LockTTL         time.Duration                   `mapstructure:"lock_ttl"`
	RenewalInterval time.Duration                   `mapstructure:"renewal_interval"`
	TLS             CoordinationRedisTLSConfig      `mapstructure:"tls"`
	Sentinel        CoordinationRedisSentinelConfig `mapstructure:"sentinel"`
	Cluster         CoordinationRedisClusterConfig  `mapstructure:"cluster"`
}

type CoordinationRedisSentinelConfig struct {
	MasterName       string   `mapstructure:"master_name"`
	Addrs            []string `mapstructure:"addrs"`
	Username         string   `mapstructure:"username"`
	Password         string   `mapstructure:"password"`
	SentinelUsername string   `mapstructure:"sentinel_username"`
	SentinelPassword string   `mapstructure:"sentinel_password"`
	DB               int      `mapstructure:"db"`
}

type CoordinationRedisClusterConfig struct {
	Addrs    []string `mapstructure:"addrs"`
	Username string   `mapstructure:"username"`
	Password string   `mapstructure:"password"`
}

type CoordinationRedisTLSConfig struct {
	CAFile             string `mapstructure:"ca_file"`
	CertFile           string `mapstructure:"cert_file"`
	KeyFile            string `mapstructure:"key_file"`
	ServerName         string `mapstructure:"server_name"`
	InsecureSkipVerify bool   `mapstructure:"insecure_skip_verify"`
}

// HealthConfig configures the Kubernetes-style health probe endpoints served by
// the serve daemon. Disabled => no probe listener is started.
type HealthConfig struct {
	Enabled       bool   `mapstructure:"enabled"`
	Listen        string `mapstructure:"listen"`
	LivenessPath  string `mapstructure:"liveness_path"`
	ReadinessPath string `mapstructure:"readiness_path"`
	StartupPath   string `mapstructure:"startup_path"`
}

type StateConfig struct {
	Driver   string                 `mapstructure:"driver"`
	Postgres PostgresStateConfig    `mapstructure:"postgres"`
	SQLite   SQLiteStateConfig      `mapstructure:"sqlite"`
	Extra    map[string]interface{} `mapstructure:",remain"`
}

type PostgresStateConfig struct {
	DSN string           `mapstructure:"dsn"`
	TLS StatePGTLSConfig `mapstructure:"tls"`
}

type StatePGTLSConfig struct {
	CAFile             string `mapstructure:"ca_file"`
	CertFile           string `mapstructure:"cert_file"`
	KeyFile            string `mapstructure:"key_file"`
	ServerName         string `mapstructure:"server_name"`
	InsecureSkipVerify bool   `mapstructure:"insecure_skip_verify"`
}

type SQLiteStateConfig struct {
	Path string `mapstructure:"path"`
}

type SinkConfig struct {
	Name            string                    `mapstructure:"name"`
	Driver          string                    `mapstructure:"driver"`
	DeadLetter      string                    `mapstructure:"dead_letter"`
	Postgres        PostgresSinkConfig        `mapstructure:"postgres"`
	Kafka           KafkaSinkConfig           `mapstructure:"kafka"`
	RabbitMQ        RabbitMQSinkConfig        `mapstructure:"rabbitmq"`
	SQS             SQSSinkConfig             `mapstructure:"sqs"`
	SNS             SNSSinkConfig             `mapstructure:"sns"`
	Stdout          StdoutSinkConfig          `mapstructure:"stdout"`
	HTTP            HTTPSinkConfig            `mapstructure:"http"`
	Feed            FeedSinkConfig            `mapstructure:"feed"`
	Composite       CompositeSinkConfig       `mapstructure:"composite"`
	GCPPubSub       GCPPubSubSinkConfig       `mapstructure:"gcp_pubsub"`
	AzureServiceBus AzureServiceBusSinkConfig `mapstructure:"azureservicebus"`
	CosmosDB        CosmosDBSinkConfig        `mapstructure:"cosmosdb"`
	DaprPubSub      DaprPubSubSinkConfig      `mapstructure:"dapr_pubsub"`
	NATS            NATSSinkConfig            `mapstructure:"nats"`
	GRPC            GRPCSinkConfig            `mapstructure:"grpc"`
	Extra           map[string]interface{}    `mapstructure:",remain"`
}

// GRPCSinkConfig configures the "grpc" sink: rss2msg dials Target and calls the
// ChangeSink.Publish RPC (proto/sink/v1) once per change.
type GRPCSinkConfig struct {
	Target    string            `mapstructure:"target"`    // gRPC dial target, e.g. host:port
	Authority string            `mapstructure:"authority"` // optional :authority / TLS server-name override
	Timeout   time.Duration     `mapstructure:"timeout"`   // per-RPC deadline; 0 -> none
	Metadata  map[string]string `mapstructure:"metadata"`  // static outgoing metadata (e.g. authorization)
	TLS       SinkTLSConfig     `mapstructure:"tls"`
}

type HTTPSinkConfig struct {
	URL          string            `mapstructure:"url"`
	Method       string            `mapstructure:"method"`        // POST (default) | PUT
	Headers      map[string]string `mapstructure:"headers"`       // static request headers
	Timeout      time.Duration     `mapstructure:"timeout"`       // 0 -> 30s
	SuccessCodes []int             `mapstructure:"success_codes"` // empty -> {200,201,202,204}
	TLS          SinkTLSConfig     `mapstructure:"tls"`
	HTTP3        bool              `mapstructure:"http3"` // dial over HTTP/3 (QUIC); requires https url
}

// SinkTLSConfig is the canonical client-TLS surface shared by the postgres,
// kafka, rabbitmq, and http sinks. It mirrors the coordinator / state-store TLS
// options. The block is "active" when Enabled is true or any field is set.
type SinkTLSConfig struct {
	// Enabled forces TLS even when no custom files are given (system roots).
	// Mainly for sinks with no URL scheme to imply TLS, e.g. kafka.
	Enabled            bool   `mapstructure:"enabled"`
	CAFile             string `mapstructure:"ca_file"`
	CertFile           string `mapstructure:"cert_file"`
	KeyFile            string `mapstructure:"key_file"`
	ServerName         string `mapstructure:"server_name"`
	InsecureSkipVerify bool   `mapstructure:"insecure_skip_verify"`
}

// Active reports whether the TLS block should be applied to the sink.
func (t SinkTLSConfig) Active() bool {
	return t.Enabled ||
		t.CAFile != "" ||
		t.CertFile != "" ||
		t.KeyFile != "" ||
		t.ServerName != "" ||
		t.InsecureSkipVerify
}

type FeedSinkConfig struct {
	Listen          string             `mapstructure:"listen"`
	PublicURL       string             `mapstructure:"public_url"`
	Title           string             `mapstructure:"title"`
	Link            string             `mapstructure:"link"`
	Description     string             `mapstructure:"description"`
	MaxItems        int                `mapstructure:"max_items"`
	RSS             FeedSurfaceConfig  `mapstructure:"rss"`
	Atom            FeedSurfaceConfig  `mapstructure:"atom"`
	MCP             FeedSurfaceConfig  `mapstructure:"mcp"`
	RenderCacheTTL  time.Duration      `mapstructure:"render_cache_ttl"`
	CacheControlTTL time.Duration      `mapstructure:"cache_control_ttl"`
	Timeouts        FeedTimeoutsConfig `mapstructure:"timeouts"`
	TLS             FeedTLSConfig      `mapstructure:"tls"`
	HTTP3           bool               `mapstructure:"http3"` // serve HTTP/3 (QUIC) alongside TCP; requires tls
	Auth            FeedAuthConfig     `mapstructure:"auth"`
	Store           FeedStoreConfig    `mapstructure:"store"`
}

// FeedSurfaceConfig is one output surface of the feed sink — RSS, Atom, or MCP.
// Each is served on the sink's shared listener at Path, behind the same TLS/auth.
// Enabled is a pointer so an omitted block ("unset") is distinguishable from an
// explicit `enabled: false`: rss/atom default on while mcp defaults off, all from
// the same zero value, resolved via On(default).
type FeedSurfaceConfig struct {
	Enabled *bool  `mapstructure:"enabled"`
	Path    string `mapstructure:"path"`
}

// On reports whether the surface is enabled, applying def when unset.
func (s FeedSurfaceConfig) On(def bool) bool {
	if s.Enabled == nil {
		return def
	}
	return *s.Enabled
}

// PathOr returns the configured path, or def when unset.
func (s FeedSurfaceConfig) PathOr(def string) string {
	if s.Path == "" {
		return def
	}
	return s.Path
}

type FeedTimeoutsConfig struct {
	ReadHeader time.Duration `mapstructure:"read_header"`
	Read       time.Duration `mapstructure:"read"`
	Write      time.Duration `mapstructure:"write"`
	Idle       time.Duration `mapstructure:"idle"`
	Shutdown   time.Duration `mapstructure:"shutdown"`
}

type FeedTLSConfig struct {
	CertFile string `mapstructure:"cert_file"`
	KeyFile  string `mapstructure:"key_file"`
}

type FeedAuthConfig struct {
	Basic       FeedBasicAuthConfig `mapstructure:"basic"`
	BearerToken string              `mapstructure:"bearer_token"`
}

type FeedBasicAuthConfig struct {
	Username string `mapstructure:"username"`
	Password string `mapstructure:"password"`
}

type FeedStoreConfig struct {
	Driver   string                  `mapstructure:"driver"`
	SQLite   FeedStoreSQLiteConfig   `mapstructure:"sqlite"`
	Postgres FeedStorePostgresConfig `mapstructure:"postgres"`
}

type FeedStoreSQLiteConfig struct {
	Path string `mapstructure:"path"`
}

type FeedStorePostgresConfig struct {
	DSN   string           `mapstructure:"dsn"`
	Table string           `mapstructure:"table"`
	TLS   StatePGTLSConfig `mapstructure:"tls"`
}

// CompositeSinkConfig configures a composite sink: a transparent fan-out to a
// list of other declared sinks by name. The composite has no retry or
// dead_letter of its own; each child keeps its own.
type CompositeSinkConfig struct {
	Children []string `mapstructure:"children"`
}

type StdoutSinkConfig struct {
	Target string `mapstructure:"target"` // stdout (default) | stderr
	Format string `mapstructure:"format"` // json (default) | pretty
}

type RabbitMQSinkConfig struct {
	URL          string        `mapstructure:"url"`
	Exchange     string        `mapstructure:"exchange"`
	ExchangeType string        `mapstructure:"exchange_type"` // direct (default) | topic | fanout | headers
	RoutingKey   string        `mapstructure:"routing_key"`
	Declare      bool          `mapstructure:"declare"`
	Durable      bool          `mapstructure:"durable"`
	Mandatory    bool          `mapstructure:"mandatory"`
	TLS          SinkTLSConfig `mapstructure:"tls"`
}

type PostgresSinkConfig struct {
	DSN   string        `mapstructure:"dsn"`
	Table string        `mapstructure:"table"`
	TLS   SinkTLSConfig `mapstructure:"tls"`
}

type KafkaSinkConfig struct {
	Brokers     []string      `mapstructure:"brokers"`
	Topic       string        `mapstructure:"topic"`
	Acks        string        `mapstructure:"acks"`        // "all" | "leader" | "none"
	Compression string        `mapstructure:"compression"` // "none" | "snappy" | "lz4" | "zstd" | "gzip"
	TLS         SinkTLSConfig `mapstructure:"tls"`
}

type SQSSinkConfig struct {
	QueueURL     string `mapstructure:"queue_url"`
	Region       string `mapstructure:"region"`
	EndpointURL  string `mapstructure:"endpoint_url"`
	MessageGroup string `mapstructure:"message_group"` // FIFO only: feed_url (default) | item_id | sink
}

type SNSSinkConfig struct {
	TopicARN     string `mapstructure:"topic_arn"`
	Region       string `mapstructure:"region"`
	EndpointURL  string `mapstructure:"endpoint_url"`
	MessageGroup string `mapstructure:"message_group"` // FIFO only: feed_url (default) | item_id | sink
}

// GCPPubSubSinkConfig configures the Google Cloud Pub/Sub sink (driver
// "gcp_pubsub"). Named for the provider so other pub/sub services (e.g. Azure
// Service Bus) can take their own driver names without collision.
type GCPPubSubSinkConfig struct {
	ProjectID   string `mapstructure:"project_id"`   // required: GCP project that owns the topic
	TopicID     string `mapstructure:"topic_id"`     // required: topic short name (must already exist)
	Endpoint    string `mapstructure:"endpoint"`     // optional: Pub/Sub emulator host
	OrderingKey string `mapstructure:"ordering_key"` // optional ordering: feed_url | item_id | sink (empty = disabled)
}

// AzureServiceBusSinkConfig configures the Azure Service Bus sink. Exactly one
// auth field (connection_string or namespace) and exactly one entity field
// (queue or topic) must be set.
type AzureServiceBusSinkConfig struct {
	ConnectionString string `mapstructure:"connection_string"` // SAS auth
	Namespace        string `mapstructure:"namespace"`         // Azure AD auth (DefaultAzureCredential)
	Queue            string `mapstructure:"queue"`             // destination queue
	Topic            string `mapstructure:"topic"`             // destination topic
}

// CosmosDBSinkConfig configures the Azure Cosmos DB (NoSQL) sink (driver
// "cosmosdb"). Changes are written as JSON documents keyed by a stable id and
// partitioned on /feed_url. Exactly one of ConnectionString or Endpoint must
// be set.
type CosmosDBSinkConfig struct {
	Endpoint         string `mapstructure:"endpoint"`          // Azure AD auth (DefaultAzureCredential)
	ConnectionString string `mapstructure:"connection_string"` // account-key auth
	Database         string `mapstructure:"database"`          // required
	Container        string `mapstructure:"container"`         // defaults to "feed_changes"
	CreateIfMissing  bool   `mapstructure:"create_if_missing"` // create db/container on startup (dev/test)
	Throughput       int32  `mapstructure:"throughput"`        // manual RU/s when creating; 0 = serverless/shared
}

// DaprPubSubSinkConfig configures the Dapr pub/sub sink (driver "dapr_pubsub").
// The sink publishes to a Dapr pub/sub component via the local sidecar; the
// underlying broker is selected by Dapr component YAML, not here.
type DaprPubSubSinkConfig struct {
	Address     string            `mapstructure:"address"`      // optional: sidecar gRPC endpoint (host:port); empty = SDK default
	PubsubName  string            `mapstructure:"pubsub_name"`  // required: Dapr pub/sub component name
	Topic       string            `mapstructure:"topic"`        // required: topic to publish to
	ContentType string            `mapstructure:"content_type"` // optional: payload content type (default application/json)
	Metadata    map[string]string `mapstructure:"metadata"`     // optional: static per-publish metadata (e.g. partition key)
}

// NATSSinkConfig configures the NATS sink (driver "nats"). At most one auth
// group (token, username/password, or creds_file) may be set.
type NATSSinkConfig struct {
	URL       string `mapstructure:"url"`        // required: one or more comma-separated NATS URLs
	Subject   string `mapstructure:"subject"`    // required: subject to publish to
	Token     string `mapstructure:"token"`      // token auth
	Username  string `mapstructure:"username"`   // user/password auth (both or neither)
	Password  string `mapstructure:"password"`   //
	CredsFile string `mapstructure:"creds_file"` // path to a NATS user credentials file (JWT + NKey seed)
	// JetStream publishes through JetStream and waits for a server ack; the
	// subject must already be bound to an existing stream (the sink never
	// creates streams).
	JetStream bool          `mapstructure:"jetstream"`
	TLS       SinkTLSConfig `mapstructure:"tls"`
}

// FeedSourceConfig is one entry in the ordered feed_sources list. Order is
// precedence: earlier entries win on URL collision. The static feeds: block is
// represented by an entry with Type "static".
type FeedSourceConfig struct {
	Type     string                   `mapstructure:"type"` // "static", "file", and "postgres" are implemented; http|sqlite|redis|s3|env are added by later plans
	Name     string                   `mapstructure:"name"` // optional; defaults to "<type>[index]"
	Path     string                   `mapstructure:"path"` // file source
	Interval time.Duration            `mapstructure:"interval"`
	Postgres PostgresFeedSourceConfig `mapstructure:"postgres"` // postgres source
}

// PostgresFeedSourceConfig configures a Postgres-backed feed source. The desired
// feed list is read from an operator-managed table; rows map to feeds via the
// url (required), interval, and sinks columns.
type PostgresFeedSourceConfig struct {
	DSN   string                `mapstructure:"dsn"`   // required
	Table string                `mapstructure:"table"` // default "feeds"; mutually exclusive with query
	Query string                `mapstructure:"query"` // raw SQL override; mutually exclusive with table
	TLS   FeedSourcePGTLSConfig `mapstructure:"tls"`
}

type FeedSourcePGTLSConfig struct {
	CAFile             string `mapstructure:"ca_file"`
	CertFile           string `mapstructure:"cert_file"`
	KeyFile            string `mapstructure:"key_file"`
	ServerName         string `mapstructure:"server_name"`
	InsecureSkipVerify bool   `mapstructure:"insecure_skip_verify"`
}

type FeedConfig struct {
	URL      string         `mapstructure:"url"`
	Interval time.Duration  `mapstructure:"interval"`
	Sinks    []string       `mapstructure:"sinks"`
	HTTP     FeedHTTPConfig `mapstructure:"http"`
}

type FeedHTTPConfig struct {
	Timeout time.Duration     `mapstructure:"timeout"`
	Headers map[string]string `mapstructure:"headers"`
}

// Defaults returns a Config populated with built-in defaults.
func Defaults() Config {
	return Config{
		Log: LogConfig{Level: "info", Format: "json"},
		Telemetry: TelemetryConfig{
			ServiceName: "rss2msg",
			Traces:      TelemetrySignalConfig{Enabled: true},
			Metrics:     TelemetrySignalConfig{Enabled: true},
			Logs:        TelemetrySignalConfig{Enabled: false},
			Prometheus:  TelemetryPrometheusConfig{Enabled: false, Listen: ":9090"},
			Graphite:    TelemetryGraphiteConfig{Enabled: false, Address: "localhost:2003", Prefix: "rss2msg", Interval: 10 * time.Second},
			Sentry: TelemetrySentryConfig{
				Enabled:          false,
				Level:            "error",
				SampleRate:       1.0,
				TracesSampleRate: 0.0,
			},
			PostHog: TelemetryPostHogConfig{
				Enabled:  false,
				Endpoint: "https://us.i.posthog.com",
				Level:    "error",
			},
			CloudWatch: TelemetryCloudWatchConfig{
				Enabled: false,
				Logs:    CloudWatchLogsConfig{Level: "info", BatchInterval: 5 * time.Second},
				Metrics: CloudWatchMetricsConfig{Namespace: "rss2msg", Interval: 60 * time.Second},
			},
		},
		HTTP:         HTTPConfig{UserAgent: "rss2msg/0.1", Timeout: 30 * time.Second},
		Retry:        RetryConfig{MaxAttempts: 3, BaseDelay: 500 * time.Millisecond, MaxDelay: 10 * time.Second},
		Runtime:      RuntimeConfig{ShutdownDrainTimeout: 30 * time.Second, RunOnceConcurrency: 0},
		Coordination: CoordinationConfig{Driver: "memory"},
		Health: HealthConfig{
			Enabled:       true,
			Listen:        ":8080",
			LivenessPath:  "/healthz",
			ReadinessPath: "/readyz",
			StartupPath:   "/startupz",
		},
	}
}
