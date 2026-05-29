package config

import "time"

type Config struct {
	Log       LogConfig       `mapstructure:"log"`
	Telemetry TelemetryConfig `mapstructure:"telemetry"`
	HTTP      HTTPConfig      `mapstructure:"http"`
	Retry     RetryConfig     `mapstructure:"retry"`
	Runtime      RuntimeConfig      `mapstructure:"runtime"`
	Coordination CoordinationConfig `mapstructure:"coordination"`
	State        StateConfig        `mapstructure:"state"`
	Sinks     []SinkConfig    `mapstructure:"sinks"`
	Feeds     []FeedConfig    `mapstructure:"feeds"`
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
	URL             string                        `mapstructure:"url"`
	LockTTL         time.Duration                 `mapstructure:"lock_ttl"`
	RenewalInterval time.Duration                 `mapstructure:"renewal_interval"`
	TLS             CoordinationRedisTLSConfig    `mapstructure:"tls"`
}

type CoordinationRedisTLSConfig struct {
	CAFile             string `mapstructure:"ca_file"`
	CertFile           string `mapstructure:"cert_file"`
	KeyFile            string `mapstructure:"key_file"`
	ServerName         string `mapstructure:"server_name"`
	InsecureSkipVerify bool   `mapstructure:"insecure_skip_verify"`
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
	Extra      map[string]interface{} `mapstructure:",remain"`
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
	}
}
