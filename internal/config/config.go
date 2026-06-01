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
	Traces      TelemetrySignalConfig     `mapstructure:"traces"`
	Metrics     TelemetrySignalConfig     `mapstructure:"metrics"`
	Logs        TelemetrySignalConfig     `mapstructure:"logs"`
	Prometheus  TelemetryPrometheusConfig `mapstructure:"prometheus"`
}

type TelemetrySignalConfig struct {
	Enabled bool `mapstructure:"enabled"`
}

type TelemetryPrometheusConfig struct {
	Enabled bool   `mapstructure:"enabled"`
	Listen  string `mapstructure:"listen"`
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
	Name       string                 `mapstructure:"name"`
	Driver     string                 `mapstructure:"driver"`
	DeadLetter string                 `mapstructure:"dead_letter"`
	Postgres   PostgresSinkConfig     `mapstructure:"postgres"`
	Kafka      KafkaSinkConfig        `mapstructure:"kafka"`
	RabbitMQ   RabbitMQSinkConfig     `mapstructure:"rabbitmq"`
	SQS        SQSSinkConfig          `mapstructure:"sqs"`
	SNS        SNSSinkConfig          `mapstructure:"sns"`
	Stdout     StdoutSinkConfig       `mapstructure:"stdout"`
	HTTP       HTTPSinkConfig         `mapstructure:"http"`
	Feed       FeedSinkConfig         `mapstructure:"feed"`
	Composite  CompositeSinkConfig    `mapstructure:"composite"`
	GCPPubSub  GCPPubSubSinkConfig    `mapstructure:"gcp_pubsub"`
	Extra      map[string]interface{} `mapstructure:",remain"`
}

type HTTPSinkConfig struct {
	URL          string            `mapstructure:"url"`
	Method       string            `mapstructure:"method"`        // POST (default) | PUT
	Headers      map[string]string `mapstructure:"headers"`       // static request headers
	Timeout      time.Duration     `mapstructure:"timeout"`       // 0 -> 30s
	SuccessCodes []int             `mapstructure:"success_codes"` // empty -> {200,201,202,204}
}

type FeedSinkConfig struct {
	Listen          string             `mapstructure:"listen"`
	PublicURL       string             `mapstructure:"public_url"`
	Title           string             `mapstructure:"title"`
	Link            string             `mapstructure:"link"`
	Description     string             `mapstructure:"description"`
	MaxItems        int                `mapstructure:"max_items"`
	RSSPath         string             `mapstructure:"rss_path"`
	AtomPath        string             `mapstructure:"atom_path"`
	RenderCacheTTL  time.Duration      `mapstructure:"render_cache_ttl"`
	CacheControlTTL time.Duration      `mapstructure:"cache_control_ttl"`
	Timeouts        FeedTimeoutsConfig `mapstructure:"timeouts"`
	TLS             FeedTLSConfig      `mapstructure:"tls"`
	Auth            FeedAuthConfig     `mapstructure:"auth"`
	Store           FeedStoreConfig    `mapstructure:"store"`
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
	URL          string `mapstructure:"url"`
	Exchange     string `mapstructure:"exchange"`
	ExchangeType string `mapstructure:"exchange_type"` // direct (default) | topic | fanout | headers
	RoutingKey   string `mapstructure:"routing_key"`
	Declare      bool   `mapstructure:"declare"`
	Durable      bool   `mapstructure:"durable"`
	Mandatory    bool   `mapstructure:"mandatory"`
}

type PostgresSinkConfig struct {
	DSN   string `mapstructure:"dsn"`
	Table string `mapstructure:"table"`
}

type KafkaSinkConfig struct {
	Brokers     []string `mapstructure:"brokers"`
	Topic       string   `mapstructure:"topic"`
	Acks        string   `mapstructure:"acks"`        // "all" | "leader" | "none"
	Compression string   `mapstructure:"compression"` // "none" | "snappy" | "lz4" | "zstd" | "gzip"
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

// FeedSourceConfig is one entry in the ordered feed_sources list. Order is
// precedence: earlier entries win on URL collision. The static feeds: block is
// represented by an entry with Type "static".
type FeedSourceConfig struct {
	Type     string        `mapstructure:"type"` // "static" and "file" are implemented; http|postgres|sqlite|redis|s3|env are added by later plans
	Name     string        `mapstructure:"name"` // optional; defaults to "<type>[index]"
	Path     string        `mapstructure:"path"` // file source
	Interval time.Duration `mapstructure:"interval"`
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
