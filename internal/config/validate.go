package config

import (
	"fmt"
	"net"
	"net/textproto"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/labels"
)

// pgKeywordSSLModeRE matches `sslmode=<value>` in pgx keyword-form DSNs,
// tolerating whitespace around the `=` (which pgx itself accepts) and only
// matching at word boundaries so substrings like `mysslmode=` don't false-
// positive.
var pgKeywordSSLModeRE = regexp.MustCompile(`(?i)\bsslmode\s*=\s*(\S+)`)

var reservedHeaders = map[string]struct{}{
	textproto.CanonicalMIMEHeaderKey("If-Modified-Since"): {},
	textproto.CanonicalMIMEHeaderKey("If-None-Match"):     {},
}

var knownStateDrivers = map[string]struct{}{
	"postgres": {},
	"sqlite":   {},
	"dynamodb": {},
	"cosmosdb": {},
}

// knownZerologLevels is the set of level names zerolog.ParseLevel accepts; used
// to validate telemetry.sentry.level without coupling config to zerolog.
var knownZerologLevels = map[string]struct{}{
	"trace":    {},
	"debug":    {},
	"info":     {},
	"warn":     {},
	"error":    {},
	"fatal":    {},
	"panic":    {},
	"disabled": {},
}

var knownSinkDrivers = map[string]struct{}{
	"postgres":        {},
	"kafka":           {},
	"amqp091":         {},
	"amqp10":          {},
	"rabbitmq_stream": {},
	"sqs":             {},
	"sns":             {},
	"dynamodb":        {},
	"stdout":          {},
	"http":            {},
	"grpc":            {},
	"feed":            {},
	"composite":       {},
	"gcp_pubsub":      {},
	"azureservicebus": {},
	"cosmosdb":        {},
	"dapr_pubsub":     {},
	"nats":            {},
}

var knownFeedStoreDrivers = map[string]struct{}{
	"memory":   {},
	"sqlite":   {},
	"postgres": {},
}

var knownHTTPSinkMethods = map[string]struct{}{
	"":     {}, // empty -> POST default
	"POST": {},
	"PUT":  {},
}

var knownStdoutTargets = map[string]struct{}{
	"":       {},
	"stdout": {},
	"stderr": {},
}

var knownStdoutFormats = map[string]struct{}{
	"":       {},
	"json":   {},
	"pretty": {},
}

var knownSQSMessageGroups = map[string]struct{}{
	"feed_url": {},
	"item_id":  {},
	"sink":     {},
}

var knownGCPPubSubOrderingKeys = map[string]struct{}{
	"feed_url": {},
	"item_id":  {},
	"sink":     {},
}

var knownRabbitMQExchangeTypes = map[string]struct{}{
	"direct":  {},
	"topic":   {},
	"fanout":  {},
	"headers": {},
}

var knownCoordinationDrivers = map[string]struct{}{
	"":         {},
	"memory":   {},
	"postgres": {},
	"redis":    {},
	"dynamodb": {},
	"cosmosdb": {},
}

// Validate checks config invariants. It returns a slice of non-fatal
// warnings (safe to ignore but worth surfacing) alongside the first fatal
// error. A nil error means the config is usable; warnings may still be present.
func Validate(c Config) ([]string, error) {
	var warnings []string
	return validate(&warnings, c)
}

// validate does the work; helpers append to *warnings.
func validate(warnings *[]string, c Config) ([]string, error) {
	if _, ok := knownStateDrivers[c.State.Driver]; !ok {
		return *warnings, fmt.Errorf("state.driver %q is not supported", c.State.Driver)
	}
	switch c.State.Driver {
	case "postgres":
		dsn := strings.TrimSpace(c.State.Postgres.DSN)
		if dsn == "" {
			return *warnings, fmt.Errorf("state.postgres.dsn is required when state.driver=postgres")
		}
		stls := c.State.Postgres.TLS
		tlsConfigured := stls.CAFile != "" || stls.CertFile != "" || stls.KeyFile != "" ||
			stls.ServerName != "" || stls.InsecureSkipVerify
		if tlsConfigured && pgSSLModeIsDisable(dsn) {
			return *warnings, fmt.Errorf("state.postgres.tls is set but the DSN has sslmode=disable; remove sslmode=disable or drop the tls block")
		}
		if (stls.CertFile == "") != (stls.KeyFile == "") {
			return *warnings, fmt.Errorf("state.postgres.tls.cert_file and key_file must both be set or both empty")
		}
	case "sqlite":
		if strings.TrimSpace(c.State.SQLite.Path) == "" {
			return *warnings, fmt.Errorf("state.sqlite.path is required when state.driver=sqlite")
		}
	case "dynamodb":
		if strings.TrimSpace(c.State.DynamoDB.Table) == "" {
			return *warnings, fmt.Errorf("state.dynamodb.table is required when state.driver=dynamodb")
		}
	case "cosmosdb":
		d := c.State.CosmosDB
		hasEndpoint := strings.TrimSpace(d.Endpoint) != ""
		hasConn := strings.TrimSpace(d.ConnectionString) != ""
		switch {
		case !hasEndpoint && !hasConn:
			return *warnings, fmt.Errorf("state.cosmosdb: one of endpoint or connection_string is required when state.driver=cosmosdb")
		case hasEndpoint && hasConn:
			return *warnings, fmt.Errorf("state.cosmosdb: endpoint and connection_string are mutually exclusive")
		}
		if strings.TrimSpace(d.Database) == "" {
			return *warnings, fmt.Errorf("state.cosmosdb.database is required when state.driver=cosmosdb")
		}
	}

	// Unified state retention (item_ttl) + SQL sweep cadence (cleanup_interval).
	if c.State.ItemTTL < 0 {
		return *warnings, fmt.Errorf("state.item_ttl %v must not be negative", c.State.ItemTTL)
	}
	sqlCleanup := map[string]time.Duration{
		"sqlite":   c.State.SQLite.CleanupInterval,
		"postgres": c.State.Postgres.CleanupInterval,
	}
	for drv, iv := range sqlCleanup {
		if c.State.Driver != drv {
			continue
		}
		if iv < 0 {
			return *warnings, fmt.Errorf("state.%s.cleanup_interval %v must not be negative", drv, iv)
		}
		if iv > 0 && c.State.ItemTTL == 0 {
			return *warnings, fmt.Errorf("state.%s.cleanup_interval is set but state.item_ttl is 0 (cleanup disabled)", drv)
		}
	}
	// DynamoDB needs the attribute name to write the expiry epoch.
	if c.State.Driver == "dynamodb" && c.State.ItemTTL > 0 &&
		strings.TrimSpace(c.State.DynamoDB.TTLAttribute) == "" {
		return *warnings, fmt.Errorf("state.dynamodb.ttl_attribute is required when state.item_ttl is set")
	}
	// Short TTLs risk pruning items still in the feed (duplicate re-publish).
	if c.State.ItemTTL > 0 && c.State.ItemTTL < time.Hour {
		*warnings = append(*warnings,
			fmt.Sprintf("state.item_ttl %v is very short; items still present in a feed may be pruned and re-published", c.State.ItemTTL))
	}

	if _, ok := knownCoordinationDrivers[c.Coordination.Driver]; !ok {
		return *warnings, fmt.Errorf("coordination.driver %q is not supported", c.Coordination.Driver)
	}
	if c.Coordination.Driver == "postgres" {
		dsn := strings.TrimSpace(c.Coordination.Postgres.DSN)
		if dsn == "" {
			dsn = strings.TrimSpace(c.State.Postgres.DSN)
		}
		if dsn == "" {
			return *warnings, fmt.Errorf("coordination.postgres.dsn (or state.postgres.dsn fallback) is required when coordination.driver=postgres")
		}
		tls := c.Coordination.Postgres.TLS
		tlsConfigured := tls.CAFile != "" || tls.CertFile != "" || tls.KeyFile != "" ||
			tls.ServerName != "" || tls.InsecureSkipVerify
		if tlsConfigured {
			if pgSSLModeIsDisable(dsn) {
				return *warnings, fmt.Errorf("coordination.postgres.tls is set but the DSN has sslmode=disable; remove sslmode=disable or drop the tls block")
			}
		}
		if (tls.CertFile == "") != (tls.KeyFile == "") {
			return *warnings, fmt.Errorf("coordination.postgres.tls.cert_file and key_file must both be set or both empty")
		}
	}
	if c.Coordination.Driver == "redis" {
		r := c.Coordination.Redis
		mode := r.Mode
		if mode == "" {
			mode = "single"
		}
		sentinelSet := r.Sentinel.MasterName != "" || len(r.Sentinel.Addrs) > 0
		clusterSet := len(r.Cluster.Addrs) > 0

		switch mode {
		case "single":
			if sentinelSet {
				return *warnings, fmt.Errorf("coordination.redis.sentinel must not be set when mode=single")
			}
			if clusterSet {
				return *warnings, fmt.Errorf("coordination.redis.cluster must not be set when mode=single")
			}
			raw := strings.TrimSpace(r.URL)
			if raw == "" {
				return *warnings, fmt.Errorf("coordination.redis.url is required when coordination.driver=redis and mode=single")
			}
			if _, err := redisparseURL(raw); err != nil {
				safe := raw
				if u, perr := url.Parse(raw); perr == nil {
					safe = u.Redacted()
				}
				return *warnings, fmt.Errorf("coordination.redis.url %q is not parseable: %w", safe, err)
			}
		case "sentinel":
			if strings.TrimSpace(r.URL) != "" {
				return *warnings, fmt.Errorf("coordination.redis.url must not be set when mode=sentinel (use sentinel.addrs)")
			}
			if clusterSet {
				return *warnings, fmt.Errorf("coordination.redis.cluster must not be set when mode=sentinel")
			}
			if strings.TrimSpace(r.Sentinel.MasterName) == "" {
				return *warnings, fmt.Errorf("coordination.redis.sentinel.master_name is required when mode=sentinel")
			}
			if len(r.Sentinel.Addrs) == 0 {
				return *warnings, fmt.Errorf("coordination.redis.sentinel.addrs is required when mode=sentinel")
			}
		case "cluster":
			if strings.TrimSpace(r.URL) != "" {
				return *warnings, fmt.Errorf("coordination.redis.url must not be set when mode=cluster (use cluster.addrs)")
			}
			if sentinelSet {
				return *warnings, fmt.Errorf("coordination.redis.sentinel must not be set when mode=cluster")
			}
			if len(r.Cluster.Addrs) == 0 {
				return *warnings, fmt.Errorf("coordination.redis.cluster.addrs is required when mode=cluster")
			}
		default:
			return *warnings, fmt.Errorf("coordination.redis.mode %q is not supported (want single, sentinel, or cluster)", r.Mode)
		}

		// Shared TTL / renewal checks.
		if ttl := r.LockTTL; ttl != 0 && ttl < time.Second {
			return *warnings, fmt.Errorf("coordination.redis.lock_ttl %v is below the 1s minimum", ttl)
		}
		if ri := r.RenewalInterval; ri != 0 {
			ttl := r.LockTTL
			if ttl == 0 {
				ttl = 30 * time.Second
			}
			if ri >= ttl {
				return *warnings, fmt.Errorf("coordination.redis.renewal_interval %v must be less than lock_ttl %v", ri, ttl)
			}
		}

		// Shared TLS checks.
		tls := r.TLS
		tlsConfigured := tls.CAFile != "" || tls.CertFile != "" || tls.KeyFile != "" ||
			tls.ServerName != "" || tls.InsecureSkipVerify
		if tlsConfigured && mode == "single" {
			u, _ := url.Parse(strings.TrimSpace(r.URL))
			if u == nil || u.Scheme != "rediss" {
				return *warnings, fmt.Errorf("coordination.redis.tls is only valid when coordination.redis.url uses the rediss:// scheme")
			}
		}
		if (tls.CertFile == "") != (tls.KeyFile == "") {
			return *warnings, fmt.Errorf("coordination.redis.tls.cert_file and key_file must both be set or both empty")
		}
	}
	if c.Coordination.Driver == "dynamodb" {
		d := c.Coordination.DynamoDB
		if strings.TrimSpace(d.Table) == "" {
			return *warnings, fmt.Errorf("coordination.dynamodb.table is required when coordination.driver=dynamodb")
		}
		if d.LeaseDuration != 0 && d.LeaseDuration < time.Second {
			return *warnings, fmt.Errorf("coordination.dynamodb.lease_duration %v is below the 1s minimum", d.LeaseDuration)
		}
	}
	if c.Coordination.Driver == "cosmosdb" {
		d := c.Coordination.CosmosDB
		hasEndpoint := strings.TrimSpace(d.Endpoint) != ""
		hasConn := strings.TrimSpace(d.ConnectionString) != ""
		switch {
		case !hasEndpoint && !hasConn:
			return *warnings, fmt.Errorf("coordination.cosmosdb: one of endpoint or connection_string is required when coordination.driver=cosmosdb")
		case hasEndpoint && hasConn:
			return *warnings, fmt.Errorf("coordination.cosmosdb: endpoint and connection_string are mutually exclusive")
		}
		if strings.TrimSpace(d.Database) == "" {
			return *warnings, fmt.Errorf("coordination.cosmosdb.database is required when coordination.driver=cosmosdb")
		}
		if d.LeaseDuration != 0 && d.LeaseDuration < time.Second {
			return *warnings, fmt.Errorf("coordination.cosmosdb.lease_duration %v is below the 1s minimum", d.LeaseDuration)
		}
	}

	if a := c.Coordination.Assignment; a.Enabled {
		if a.Strategy != "" && a.Strategy != "rendezvous" {
			return *warnings, fmt.Errorf("coordination.assignment.strategy %q is not supported (only \"rendezvous\")", a.Strategy)
		}
		if a.HeartbeatInterval < 0 || a.MemberTTL < 0 || a.RebalanceGrace < 0 {
			return *warnings, fmt.Errorf("coordination.assignment durations must be non-negative")
		}
		hb := a.HeartbeatInterval
		if hb == 0 {
			hb = 10 * time.Second
		}
		ttl := a.MemberTTL
		if ttl == 0 {
			ttl = 30 * time.Second
		}
		if ttl <= hb {
			return *warnings, fmt.Errorf("coordination.assignment.member_ttl (%s) must exceed heartbeat_interval (%s)", ttl, hb)
		}
		if c.Coordination.Driver == "" || c.Coordination.Driver == "memory" {
			*warnings = append(*warnings, "coordination.assignment.enabled has no effect with the memory coordinator (single instance); use redis/postgres/dynamodb/cosmosdb for multi-instance assignment")
		}
	}

	if c.Runtime.DeliverTimeout < 0 {
		return *warnings, fmt.Errorf("runtime.deliver_timeout %v must not be negative (0 disables it)", c.Runtime.DeliverTimeout)
	}

	// A distributed coordinator (redis/postgres) implies multiple instances,
	// but the SQLite state store is a local per-instance file. The coordinator
	// only serialises polling; dedup lives in the state store, so each instance
	// keeps its own seen-items set and republishes everything other instances
	// already sent. Mirror the feed-sink multi-instance warning below.
	if c.State.Driver == "sqlite" &&
		(c.Coordination.Driver == "redis" || c.Coordination.Driver == "postgres" || c.Coordination.Driver == "dynamodb" || c.Coordination.Driver == "cosmosdb") {
		*warnings = append(*warnings, fmt.Sprintf(
			"coordination.driver=%q looks multi-instance but state.driver=%q is a per-instance local file; cross-instance dedup is broken and items will be republished (use state.driver: postgres)",
			c.Coordination.Driver, c.State.Driver))
	}

	if s := c.Telemetry.Sentry; s.Enabled {
		if s.SampleRate < 0 || s.SampleRate > 1 {
			return *warnings, fmt.Errorf("telemetry.sentry.sample_rate %v must be within [0.0, 1.0]", s.SampleRate)
		}
		if s.TracesSampleRate < 0 || s.TracesSampleRate > 1 {
			return *warnings, fmt.Errorf("telemetry.sentry.traces_sample_rate %v must be within [0.0, 1.0]", s.TracesSampleRate)
		}
		if lvl := strings.TrimSpace(s.Level); lvl != "" {
			if _, ok := knownZerologLevels[strings.ToLower(lvl)]; !ok {
				return *warnings, fmt.Errorf("telemetry.sentry.level %q is not a valid level (want one of trace, debug, info, warn, error, fatal, panic, disabled)", s.Level)
			}
		}
	}

	if p := c.Telemetry.PostHog; p.Enabled {
		if p.FlushInterval < 0 {
			return *warnings, fmt.Errorf("telemetry.posthog.flush_interval %v must not be negative", p.FlushInterval)
		}
		if lvl := strings.TrimSpace(p.Level); lvl != "" {
			if _, ok := knownZerologLevels[strings.ToLower(lvl)]; !ok {
				return *warnings, fmt.Errorf("telemetry.posthog.level %q is not a valid level (want one of trace, debug, info, warn, error, fatal, panic, disabled)", p.Level)
			}
		}
	}

	if c.Health.Enabled {
		if strings.TrimSpace(c.Health.Listen) == "" {
			return *warnings, fmt.Errorf("health.listen is required when health.enabled=true")
		}
		paths := map[string]string{
			"health.liveness_path":  c.Health.LivenessPath,
			"health.readiness_path": c.Health.ReadinessPath,
			"health.startup_path":   c.Health.StartupPath,
		}
		for key, p := range paths {
			if p == "" || p[0] != '/' {
				return *warnings, fmt.Errorf("%s %q must start with /", key, p)
			}
		}
		seen := make(map[string]struct{}, len(paths))
		for _, p := range []string{c.Health.LivenessPath, c.Health.ReadinessPath, c.Health.StartupPath} {
			if _, dup := seen[p]; dup {
				return *warnings, fmt.Errorf("health probe paths must be distinct (%q is reused)", p)
			}
			seen[p] = struct{}{}
		}
		if c.Telemetry.Prometheus.Enabled && c.Health.Listen == c.Telemetry.Prometheus.Listen {
			*warnings = append(*warnings, fmt.Sprintf("health.listen %q is the same as telemetry.prometheus.listen; one server will fail to bind", c.Health.Listen))
		}
	}

	if c.Telemetry.Graphite.Enabled {
		if strings.TrimSpace(c.Telemetry.Graphite.Address) == "" {
			return *warnings, fmt.Errorf("telemetry.graphite.address is required when telemetry.graphite.enabled=true")
		}
		if c.Telemetry.Graphite.Interval < 0 {
			return *warnings, fmt.Errorf("telemetry.graphite.interval must not be negative")
		}
		if !c.Telemetry.Metrics.Enabled {
			*warnings = append(*warnings, "telemetry.graphite is enabled but telemetry.metrics.enabled=false; no metrics will be pushed")
		}
	}

	if cw := c.Telemetry.CloudWatch; cw.Enabled {
		if cw.Logs.Enabled {
			if strings.TrimSpace(cw.Logs.LogGroup) == "" {
				return *warnings, fmt.Errorf("telemetry.cloudwatch.logs.log_group is required when telemetry.cloudwatch.logs.enabled=true")
			}
			if lvl := strings.TrimSpace(cw.Logs.Level); lvl != "" {
				if _, ok := knownZerologLevels[strings.ToLower(lvl)]; !ok {
					return *warnings, fmt.Errorf("telemetry.cloudwatch.logs.level %q is not a valid level (want one of trace, debug, info, warn, error, fatal, panic, disabled)", cw.Logs.Level)
				}
			}
			if cw.Logs.BatchInterval < 0 {
				return *warnings, fmt.Errorf("telemetry.cloudwatch.logs.batch_interval must not be negative")
			}
		}
		if cw.Metrics.Enabled {
			if cw.Metrics.Interval < 0 {
				return *warnings, fmt.Errorf("telemetry.cloudwatch.metrics.interval must not be negative")
			}
			if !c.Telemetry.Metrics.Enabled {
				*warnings = append(*warnings, "telemetry.cloudwatch.metrics is enabled but telemetry.metrics.enabled=false; no metrics will be pushed")
			}
		}
		if !cw.Logs.Enabled && !cw.Metrics.Enabled {
			*warnings = append(*warnings, "telemetry.cloudwatch.enabled=true but neither logs nor metrics is enabled; CloudWatch will do nothing")
		}
	}

	if len(c.Sinks) == 0 {
		return *warnings, fmt.Errorf("at least one sink must be declared")
	}
	names := make(map[string]struct{}, len(c.Sinks))
	for i, s := range c.Sinks {
		if strings.TrimSpace(s.Name) == "" {
			return *warnings, fmt.Errorf("sinks[%d].name is required", i)
		}
		if _, dup := names[s.Name]; dup {
			return *warnings, fmt.Errorf("duplicate sink name %q", s.Name)
		}
		names[s.Name] = struct{}{}
		if _, ok := knownSinkDrivers[s.Driver]; !ok {
			return *warnings, fmt.Errorf("sinks[%d].driver %q is not supported", i, s.Driver)
		}
	}
	// Build a map from sink name to driver for dead-letter driver checks,
	// and a composite-children map used for cycle detection.
	sinkDrivers := make(map[string]string, len(c.Sinks))
	compositeChildren := make(map[string][]string)
	for _, s := range c.Sinks {
		sinkDrivers[s.Name] = s.Driver
		if s.Driver == "composite" {
			compositeChildren[s.Name] = s.Composite.Children
		}
	}
	for i, s := range c.Sinks {
		// Check the composite dead_letter guard before the generic dead_letter
		// validation so a composite that mistakenly sets dead_letter always gets
		// the actionable "not allowed on a composite" message, even when the
		// target itself is unknown or invalid.
		if s.Driver == "composite" && s.DeadLetter != "" {
			return *warnings, fmt.Errorf("sinks[%d] (composite %q): dead_letter is not allowed on a composite; configure dead_letter on each child instead", i, s.Name)
		}
		if s.DeadLetter != "" {
			if s.DeadLetter == s.Name {
				return *warnings, fmt.Errorf("sinks[%d].dead_letter must not refer to its own sink %q", i, s.Name)
			}
			if _, ok := names[s.DeadLetter]; !ok {
				return *warnings, fmt.Errorf("sinks[%d].dead_letter %q does not refer to a declared sink", i, s.DeadLetter)
			}
			if sinkDrivers[s.DeadLetter] == "feed" {
				return *warnings, fmt.Errorf("sinks[%d].dead_letter %q: a feed sink cannot be used as a dead-letter target", i, s.DeadLetter)
			}
		}
		switch s.Driver {
		case "sqs":
			if strings.TrimSpace(s.SQS.QueueURL) == "" {
				return *warnings, fmt.Errorf("sinks[%d] (sqs %q): sqs.queue_url is required", i, s.Name)
			}
			fifo := strings.HasSuffix(s.SQS.QueueURL, ".fifo")
			if mg := s.SQS.MessageGroup; mg != "" {
				if _, ok := knownSQSMessageGroups[mg]; !ok {
					return *warnings, fmt.Errorf("sinks[%d] (sqs %q): unknown message_group %q (want one of feed_url, item_id, sink)", i, s.Name, mg)
				}
				if !fifo {
					return *warnings, fmt.Errorf("sinks[%d] (sqs %q): message_group is only valid for FIFO queues (queue_url must end with .fifo)", i, s.Name)
				}
			}
		case "sns":
			if strings.TrimSpace(s.SNS.TopicARN) == "" {
				return *warnings, fmt.Errorf("sinks[%d] (sns %q): sns.topic_arn is required", i, s.Name)
			}
			fifo := strings.HasSuffix(s.SNS.TopicARN, ".fifo")
			if mg := s.SNS.MessageGroup; mg != "" {
				if _, ok := knownSQSMessageGroups[mg]; !ok {
					return *warnings, fmt.Errorf("sinks[%d] (sns %q): unknown message_group %q (want one of feed_url, item_id, sink)", i, s.Name, mg)
				}
				if !fifo {
					return *warnings, fmt.Errorf("sinks[%d] (sns %q): message_group is only valid for FIFO topics (topic_arn must end with .fifo)", i, s.Name)
				}
			}
		case "dynamodb":
			if strings.TrimSpace(s.DynamoDB.Table) == "" {
				return *warnings, fmt.Errorf("sinks[%d] (dynamodb %q): dynamodb.table is required", i, s.Name)
			}
			if strings.TrimSpace(s.DynamoDB.TTLAttribute) == "" && s.DynamoDB.ItemTTL != 0 {
				return *warnings, fmt.Errorf("sinks[%d] (dynamodb %q): item_ttl set but ttl_attribute is empty", i, s.Name)
			}
		case "gcp_pubsub":
			if strings.TrimSpace(s.GCPPubSub.ProjectID) == "" {
				return *warnings, fmt.Errorf("sinks[%d] (gcp_pubsub %q): gcp_pubsub.project_id is required", i, s.Name)
			}
			if strings.TrimSpace(s.GCPPubSub.TopicID) == "" {
				return *warnings, fmt.Errorf("sinks[%d] (gcp_pubsub %q): gcp_pubsub.topic_id is required", i, s.Name)
			}
			if ok := s.GCPPubSub.OrderingKey; ok != "" {
				if _, found := knownGCPPubSubOrderingKeys[ok]; !found {
					return *warnings, fmt.Errorf("sinks[%d] (gcp_pubsub %q): unknown ordering_key %q (want one of feed_url, item_id, sink)", i, s.Name, ok)
				}
			}
		case "stdout":
			if _, ok := knownStdoutTargets[s.Stdout.Target]; !ok {
				return *warnings, fmt.Errorf("sinks[%d] (stdout %q): unknown target %q (want stdout or stderr)", i, s.Name, s.Stdout.Target)
			}
			if _, ok := knownStdoutFormats[s.Stdout.Format]; !ok {
				return *warnings, fmt.Errorf("sinks[%d] (stdout %q): unknown format %q (want json or pretty)", i, s.Name, s.Stdout.Format)
			}
		case "http":
			raw := strings.TrimSpace(s.HTTP.URL)
			if raw == "" {
				return *warnings, fmt.Errorf("sinks[%d] (http %q): http.url is required", i, s.Name)
			}
			u, err := url.Parse(raw)
			if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
				return *warnings, fmt.Errorf("sinks[%d] (http %q): http.url %q is not a valid http(s) URL", i, s.Name, raw)
			}
			if s.HTTP.HTTP3 && u.Scheme != "https" {
				return *warnings, fmt.Errorf("sinks[%d] (http %q): http3 requires an https:// url (HTTP/3 is TLS-only)", i, s.Name)
			}
			if _, ok := knownHTTPSinkMethods[s.HTTP.Method]; !ok {
				return *warnings, fmt.Errorf("sinks[%d] (http %q): unknown method %q (want POST or PUT)", i, s.Name, s.HTTP.Method)
			}
			for _, c := range s.HTTP.SuccessCodes {
				if c < 100 || c > 599 {
					return *warnings, fmt.Errorf("sinks[%d] (http %q): success_code %d is out of range 100-599", i, s.Name, c)
				}
			}
		case "grpc":
			if strings.TrimSpace(s.GRPC.Target) == "" {
				return *warnings, fmt.Errorf("sinks[%d] (grpc %q): grpc.target is required", i, s.Name)
			}
			if s.GRPC.Timeout < 0 {
				return *warnings, fmt.Errorf("sinks[%d] (grpc %q): grpc.timeout must not be negative", i, s.Name)
			}
			for k := range s.GRPC.Metadata {
				lk := strings.ToLower(strings.TrimSpace(k))
				if lk == "" {
					return *warnings, fmt.Errorf("sinks[%d] (grpc %q): grpc.metadata has an empty key", i, s.Name)
				}
				if strings.HasPrefix(lk, ":") || strings.HasPrefix(lk, "grpc-") || lk == "content-type" {
					return *warnings, fmt.Errorf("sinks[%d] (grpc %q): grpc.metadata key %q is reserved", i, s.Name, k)
				}
			}
		case "amqp091":
			if strings.TrimSpace(s.AMQP091.URL) == "" {
				return *warnings, fmt.Errorf("sinks[%d] (amqp091 %q): amqp091.url is required", i, s.Name)
			}
			if et := s.AMQP091.ExchangeType; et != "" {
				if _, ok := knownRabbitMQExchangeTypes[et]; !ok {
					return *warnings, fmt.Errorf("sinks[%d] (amqp091 %q): unknown exchange_type %q (want one of direct, topic, fanout, headers)", i, s.Name, et)
				}
			}
			if s.AMQP091.Declare && strings.TrimSpace(s.AMQP091.Exchange) == "" {
				return *warnings, fmt.Errorf("sinks[%d] (amqp091 %q): declare=true requires a non-empty exchange (the default exchange cannot be declared)", i, s.Name)
			}
		case "amqp10":
			if strings.TrimSpace(s.AMQP10.URL) == "" {
				return *warnings, fmt.Errorf("sinks[%d] (amqp10 %q): amqp10.url is required", i, s.Name)
			}
			if strings.TrimSpace(s.AMQP10.Target) == "" {
				return *warnings, fmt.Errorf("sinks[%d] (amqp10 %q): amqp10.target is required", i, s.Name)
			}
		case "rabbitmq_stream":
			if strings.TrimSpace(s.RabbitMQStream.Stream) == "" {
				return *warnings, fmt.Errorf("sinks[%d] (rabbitmq_stream %q): rabbitmq_stream.stream is required", i, s.Name)
			}
			if len(s.RabbitMQStream.URIs) == 0 && strings.TrimSpace(s.RabbitMQStream.URL) == "" {
				return *warnings, fmt.Errorf("sinks[%d] (rabbitmq_stream %q): rabbitmq_stream.uris or rabbitmq_stream.url is required", i, s.Name)
			}
		case "azureservicebus":
			a := s.AzureServiceBus
			hasConn := strings.TrimSpace(a.ConnectionString) != ""
			hasNS := strings.TrimSpace(a.Namespace) != ""
			switch {
			case !hasConn && !hasNS:
				return *warnings, fmt.Errorf("sinks[%d] (azureservicebus %q): one of connection_string or namespace is required", i, s.Name)
			case hasConn && hasNS:
				return *warnings, fmt.Errorf("sinks[%d] (azureservicebus %q): connection_string and namespace are mutually exclusive", i, s.Name)
			}
			hasQueue := strings.TrimSpace(a.Queue) != ""
			hasTopic := strings.TrimSpace(a.Topic) != ""
			switch {
			case !hasQueue && !hasTopic:
				return *warnings, fmt.Errorf("sinks[%d] (azureservicebus %q): one of queue or topic is required", i, s.Name)
			case hasQueue && hasTopic:
				return *warnings, fmt.Errorf("sinks[%d] (azureservicebus %q): queue and topic are mutually exclusive", i, s.Name)
			}
		case "cosmosdb":
			c := s.CosmosDB
			hasEndpoint := strings.TrimSpace(c.Endpoint) != ""
			hasConn := strings.TrimSpace(c.ConnectionString) != ""
			switch {
			case !hasEndpoint && !hasConn:
				return *warnings, fmt.Errorf("sinks[%d] (cosmosdb %q): one of endpoint or connection_string is required", i, s.Name)
			case hasEndpoint && hasConn:
				return *warnings, fmt.Errorf("sinks[%d] (cosmosdb %q): endpoint and connection_string are mutually exclusive", i, s.Name)
			}
			if strings.TrimSpace(c.Database) == "" {
				return *warnings, fmt.Errorf("sinks[%d] (cosmosdb %q): cosmosdb.database is required", i, s.Name)
			}
			if c.Throughput < 0 {
				return *warnings, fmt.Errorf("sinks[%d] (cosmosdb %q): cosmosdb.throughput must not be negative", i, s.Name)
			}
		case "dapr_pubsub":
			if strings.TrimSpace(s.DaprPubSub.PubsubName) == "" {
				return *warnings, fmt.Errorf("sinks[%d] (dapr_pubsub %q): dapr_pubsub.pubsub_name is required", i, s.Name)
			}
			if strings.TrimSpace(s.DaprPubSub.Topic) == "" {
				return *warnings, fmt.Errorf("sinks[%d] (dapr_pubsub %q): dapr_pubsub.topic is required", i, s.Name)
			}
		case "nats":
			n := s.NATS
			if strings.TrimSpace(n.URL) == "" {
				return *warnings, fmt.Errorf("sinks[%d] (nats %q): nats.url is required", i, s.Name)
			}
			if strings.TrimSpace(n.Subject) == "" {
				return *warnings, fmt.Errorf("sinks[%d] (nats %q): nats.subject is required", i, s.Name)
			}
			if (strings.TrimSpace(n.Username) == "") != (strings.TrimSpace(n.Password) == "") {
				return *warnings, fmt.Errorf("sinks[%d] (nats %q): username and password must both be set or both empty", i, s.Name)
			}
			authGroups := 0
			if strings.TrimSpace(n.Token) != "" {
				authGroups++
			}
			if strings.TrimSpace(n.Username) != "" {
				authGroups++
			}
			if strings.TrimSpace(n.CredsFile) != "" {
				authGroups++
			}
			if authGroups > 1 {
				return *warnings, fmt.Errorf("sinks[%d] (nats %q): at most one of token, username/password, or creds_file may be set", i, s.Name)
			}
		case "composite":
			if len(s.Composite.Children) == 0 {
				return *warnings, fmt.Errorf("sinks[%d] (composite %q): composite.children is required", i, s.Name)
			}
			seen := make(map[string]struct{}, len(s.Composite.Children))
			for _, child := range s.Composite.Children {
				if child == s.Name {
					return *warnings, fmt.Errorf("sinks[%d] (composite %q): composite.children must not reference its own sink", i, s.Name)
				}
				if _, ok := names[child]; !ok {
					return *warnings, fmt.Errorf("sinks[%d] (composite %q): composite.children references unknown sink %q", i, s.Name, child)
				}
				if _, dup := seen[child]; dup {
					return *warnings, fmt.Errorf("sinks[%d] (composite %q): composite.children has a duplicate entry %q", i, s.Name, child)
				}
				seen[child] = struct{}{}
			}
			if path := compositeCycle(s.Name, compositeChildren); path != "" {
				return *warnings, fmt.Errorf("sinks[%d] (composite %q): cyclic child reference (%s)", i, s.Name, path)
			}
		case "feed":
			f := s.Feed
			if strings.TrimSpace(f.Listen) == "" {
				return *warnings, fmt.Errorf("sinks[%d] (feed %q): feed.listen is required", i, s.Name)
			}
			// 0 means "unset" → the sink applies the default (50). Reject only
			// negative values, which are never meaningful.
			if f.MaxItems < 0 {
				return *warnings, fmt.Errorf("sinks[%d] (feed %q): feed.max_items must not be negative", i, s.Name)
			}
			surfaces := []struct {
				name, path string
				enabled    bool
			}{
				{"rss", f.RSS.PathOr("/rss"), f.RSS.On(true)},
				{"atom", f.Atom.PathOr("/atom"), f.Atom.On(true)},
				{"mcp", f.MCP.PathOr("/mcp"), f.MCP.On(false)},
			}
			byPath := map[string]string{}
			anyEnabled := false
			for _, srf := range surfaces {
				if !srf.enabled {
					continue
				}
				anyEnabled = true
				if srf.path == "" || srf.path[0] != '/' {
					return *warnings, fmt.Errorf("sinks[%d] (feed %q): %s.path must start with /", i, s.Name, srf.name)
				}
				if other, ok := byPath[srf.path]; ok {
					return *warnings, fmt.Errorf("sinks[%d] (feed %q): %s.path %q collides with %s.path", i, s.Name, srf.name, srf.path, other)
				}
				byPath[srf.path] = srf.name
			}
			if !anyEnabled {
				return *warnings, fmt.Errorf("sinks[%d] (feed %q): at least one of rss/atom/mcp must be enabled", i, s.Name)
			}
			for _, raw := range []string{f.Link, f.PublicURL} {
				if raw == "" {
					continue
				}
				if u, err := url.Parse(raw); err != nil || u.Scheme == "" || u.Host == "" {
					return *warnings, fmt.Errorf("sinks[%d] (feed %q): %q is not an absolute URL", i, s.Name, raw)
				}
			}
			for _, raw := range f.TrustedProxies {
				if err := validateTrustedProxyEntry(raw); err != nil {
					return *warnings, fmt.Errorf("sinks[%d] (feed %q): trusted_proxies entry %q: %w", i, s.Name, raw, err)
				}
			}
			if (f.TLS.CertFile == "") != (f.TLS.KeyFile == "") {
				return *warnings, fmt.Errorf("sinks[%d] (feed %q): tls.cert_file and key_file must both be set or both empty", i, s.Name)
			}
			if f.HTTP3 && (f.TLS.CertFile == "" || f.TLS.KeyFile == "") {
				return *warnings, fmt.Errorf("sinks[%d] (feed %q): http3 requires tls.cert_file and key_file (HTTP/3 is TLS-only)", i, s.Name)
			}
			// Validate the top-level default and each per-surface override.
			// The default may be empty (=> public); an *override* that is
			// present must either be disabled or define a method.
			if err := validateFeedAuth(i, s.Name, "auth", f.Auth, false); err != nil {
				return *warnings, err
			}
			for _, ov := range []struct {
				label string
				a     *FeedAuthConfig
			}{{"rss.auth", f.RSS.Auth}, {"atom.auth", f.Atom.Auth}, {"mcp.auth", f.MCP.Auth}} {
				if ov.a == nil {
					continue
				}
				if err := validateFeedAuth(i, s.Name, ov.label, *ov.a, true); err != nil {
					return *warnings, err
				}
			}
			sd := storeDriverOrDefault(f.Store.Driver)
			if _, ok := knownFeedStoreDrivers[sd]; !ok {
				return *warnings, fmt.Errorf("sinks[%d] (feed %q): unknown store.driver %q", i, s.Name, f.Store.Driver)
			}
			switch sd {
			case "sqlite":
				if strings.TrimSpace(f.Store.SQLite.Path) == "" {
					return *warnings, fmt.Errorf("sinks[%d] (feed %q): store.sqlite.path is required", i, s.Name)
				}
			case "postgres":
				if strings.TrimSpace(f.Store.Postgres.DSN) == "" {
					return *warnings, fmt.Errorf("sinks[%d] (feed %q): store.postgres.dsn is required", i, s.Name)
				}
				if t := f.Store.Postgres.Table; t != "" && !isValidPGIdentifier(t) {
					return *warnings, fmt.Errorf("sinks[%d] (feed %q): invalid store.postgres.table %q", i, s.Name, t)
				}
				ptls := f.Store.Postgres.TLS
				tlsSet := ptls.CAFile != "" || ptls.CertFile != "" || ptls.KeyFile != "" || ptls.ServerName != "" || ptls.InsecureSkipVerify
				if tlsSet && pgSSLModeIsDisable(f.Store.Postgres.DSN) {
					return *warnings, fmt.Errorf("sinks[%d] (feed %q): store.postgres.tls set but DSN has sslmode=disable", i, s.Name)
				}
				if (ptls.CertFile == "") != (ptls.KeyFile == "") {
					return *warnings, fmt.Errorf("sinks[%d] (feed %q): store.postgres.tls.cert_file and key_file must both be set or both empty", i, s.Name)
				}
			}
			if sd != "postgres" && c.Coordination.Driver != "" && c.Coordination.Driver != "memory" {
				*warnings = append(*warnings, fmt.Sprintf("sink %q: coordination=%q looks multi-instance but feed window is %q; readers may get partial feeds (use store.driver: postgres)", s.Name, c.Coordination.Driver, sd))
			}
		}
	}

	// Client-TLS blocks (postgres/kafka/amqp091/http/nats sinks) require
	// cert_file and key_file to be set together for mutual-TLS.
	for i, s := range c.Sinks {
		var stls SinkTLSConfig
		switch s.Driver {
		case "postgres":
			stls = s.Postgres.TLS
		case "kafka":
			stls = s.Kafka.TLS
		case "amqp091":
			stls = s.AMQP091.TLS
		case "amqp10":
			stls = s.AMQP10.TLS
		case "rabbitmq_stream":
			stls = s.RabbitMQStream.TLS
		case "http":
			stls = s.HTTP.TLS
		case "nats":
			stls = s.NATS.TLS
		case "grpc":
			stls = s.GRPC.TLS
		default:
			continue
		}
		if (stls.CertFile == "") != (stls.KeyFile == "") {
			return *warnings, fmt.Errorf("sinks[%d] (%s %q): tls.cert_file and key_file must both be set or both empty", i, s.Driver, s.Name)
		}
	}

	// Kafka Schema Registry: url enables the feature; format is then required
	// and must be supported. format without url is an error.
	for i, s := range c.Sinks {
		if s.Driver != "kafka" {
			continue
		}
		sr := s.Kafka.SchemaRegistry
		if sr.URL == "" {
			if sr.Format != "" {
				return *warnings, fmt.Errorf("sinks[%d] (kafka %q): schema_registry.format set without schema_registry.url", i, s.Name)
			}
			continue
		}
		switch sr.Format {
		case "json", "avro", "protobuf":
			// supported
		case "":
			return *warnings, fmt.Errorf("sinks[%d] (kafka %q): schema_registry.format is required when url is set", i, s.Name)
		default:
			return *warnings, fmt.Errorf("sinks[%d] (kafka %q): schema_registry.format %q is invalid (json|avro|protobuf)", i, s.Name, sr.Format)
		}
		if sr.SchemaFile != "" {
			if _, err := os.Stat(sr.SchemaFile); err != nil {
				return *warnings, fmt.Errorf("sinks[%d] (kafka %q): schema_registry.schema_file %q: %w", i, s.Name, sr.SchemaFile, err)
			}
		}
		if (sr.BasicAuth.Username == "") != (sr.BasicAuth.Password == "") {
			return *warnings, fmt.Errorf("sinks[%d] (kafka %q): schema_registry.basic_auth username and password must both be set or both empty", i, s.Name)
		}
		if (sr.TLS.CertFile == "") != (sr.TLS.KeyFile == "") {
			return *warnings, fmt.Errorf("sinks[%d] (kafka %q): schema_registry.tls.cert_file and key_file must both be set or both empty", i, s.Name)
		}
	}

	hasDefault := false
	if _, ok := names["default"]; ok {
		hasDefault = true
	}

	if err := validateRetry("http.retry", c.HTTP.Retry); err != nil {
		return *warnings, err
	}

	if c.Heartbeat.Enabled && c.Heartbeat.Interval <= 0 {
		return *warnings, fmt.Errorf("heartbeat.interval %v must be positive when heartbeat.enabled is true", c.Heartbeat.Interval)
	}

	if len(c.Feeds) == 0 && len(c.FeedSources) == 0 {
		return *warnings, fmt.Errorf("at least one feed must be declared")
	}
	for i, f := range c.Feeds {
		if strings.TrimSpace(f.URL) == "" {
			return *warnings, fmt.Errorf("feeds[%d].url is required", i)
		}
		if f.Interval < time.Second {
			return *warnings, fmt.Errorf("feeds[%d].interval %v is below the 1s minimum", i, f.Interval)
		}
		if len(f.Sinks) == 0 {
			if !hasDefault {
				return *warnings, fmt.Errorf(`feeds[%d] has no sinks and no sink named "default" is declared`, i)
			}
		}
		for _, name := range f.Sinks {
			if _, ok := names[name]; !ok {
				return *warnings, fmt.Errorf("feeds[%d].sinks references unknown sink %q", i, name)
			}
		}
		for h := range f.HTTP.Headers {
			canon := textproto.CanonicalMIMEHeaderKey(h)
			if _, bad := reservedHeaders[canon]; bad {
				return *warnings, fmt.Errorf("feeds[%d].http.headers must not set reserved cache header %q", i, h)
			}
		}
		if err := validateRetry(fmt.Sprintf("feeds[%d].http.retry", i), f.HTTP.Retry); err != nil {
			return *warnings, err
		}
	}
	for i, s := range c.FeedSources {
		switch s.Type {
		case "static":
			// no required fields; injects the feeds: block at this position
		case "file":
			if strings.TrimSpace(s.Path) == "" {
				return *warnings, fmt.Errorf("feed_sources[%d] (file): path is required", i)
			}
		case "postgres":
			if strings.TrimSpace(s.Postgres.DSN) == "" {
				return *warnings, fmt.Errorf("feed_sources[%d] (postgres): dsn is required", i)
			}
			if s.Postgres.Table != "" && s.Postgres.Query != "" {
				return *warnings, fmt.Errorf("feed_sources[%d] (postgres): table and query are mutually exclusive", i)
			}
			if s.Postgres.Table != "" && !validSQLIdentifier(s.Postgres.Table) {
				return *warnings, fmt.Errorf("feed_sources[%d] (postgres): invalid table %q", i, s.Postgres.Table)
			}
		case "http":
			if strings.TrimSpace(s.HTTP.URL) == "" {
				return *warnings, fmt.Errorf("feed_sources[%d] (http): url is required", i)
			}
			if (s.HTTP.TLS.CertFile == "") != (s.HTTP.TLS.KeyFile == "") {
				return *warnings, fmt.Errorf("feed_sources[%d] (http): cert_file and key_file must both be set or both empty", i)
			}
			if s.HTTP.Timeout < 0 {
				return *warnings, fmt.Errorf("feed_sources[%d] (http): timeout must not be negative", i)
			}
			for h := range s.HTTP.Headers {
				canon := textproto.CanonicalMIMEHeaderKey(h)
				if _, bad := reservedHeaders[canon]; bad {
					return *warnings, fmt.Errorf("feed_sources[%d] (http): headers must not set reserved cache header %q", i, h)
				}
			}
		case "kubernetes":
			if s.Kubernetes.LabelSelector != "" {
				if _, err := labels.Parse(s.Kubernetes.LabelSelector); err != nil {
					return *warnings, fmt.Errorf("feed_sources[%d] (kubernetes): invalid label_selector: %w", i, err)
				}
			}
			if s.Kubernetes.ResyncInterval != 0 && s.Kubernetes.ResyncInterval < time.Second {
				return *warnings, fmt.Errorf("feed_sources[%d] (kubernetes): resync_interval %v is below the 1s minimum", i, s.Kubernetes.ResyncInterval)
			}
		case "":
			return *warnings, fmt.Errorf("feed_sources[%d]: type is required", i)
		default:
			return *warnings, fmt.Errorf("feed_sources[%d]: unsupported type %q", i, s.Type)
		}
		if s.Interval != 0 && s.Interval < time.Second {
			return *warnings, fmt.Errorf("feed_sources[%d].interval %v is below the 1s minimum", i, s.Interval)
		}
	}
	return *warnings, nil
}

// validSQLIdentifier reports whether s is a safe unquoted SQL identifier (used
// for feed-source table names that get interpolated into a query).
func validSQLIdentifier(s string) bool {
	if s == "" || len(s) > 63 {
		return false
	}
	for i, r := range s {
		switch {
		case r == '_':
		case r >= 'a' && r <= 'z':
		case r >= 'A' && r <= 'Z':
		case r >= '0' && r <= '9' && i > 0:
		default:
			return false
		}
	}
	return true
}

// ResolveFeedSinks returns the sink names a feed publishes to, applying the
// "no sinks -> default" fallback.
func ResolveFeedSinks(f FeedConfig) []string {
	if len(f.Sinks) > 0 {
		return f.Sinks
	}
	return []string{"default"}
}

// compositeCycle returns a "a -> b -> a" path string if the composite graph
// rooted at start contains a cycle. It only walks into children that are
// themselves composites (present as keys in childrenOf).
func compositeCycle(start string, childrenOf map[string][]string) string {
	onStack := make(map[string]bool)
	var path []string
	var dfs func(n string) bool
	dfs = func(n string) bool {
		onStack[n] = true
		path = append(path, n)
		for _, c := range childrenOf[n] {
			if _, isComposite := childrenOf[c]; !isComposite {
				continue
			}
			if onStack[c] {
				path = append(path, c)
				return true
			}
			if dfs(c) {
				return true
			}
		}
		onStack[n] = false
		path = path[:len(path)-1]
		return false
	}
	if dfs(start) {
		return strings.Join(path, " -> ")
	}
	return ""
}

// redisparseURL is a lightweight syntactic check that mirrors the subset of
// redis.ParseURL we care about at config-validate time: scheme must be
// redis or rediss, host must be non-empty, optional /<db> path must be a
// non-negative integer. The actual TLS / auth handling is done by
// redis.ParseURL inside the coord/redis package at startup.
func redisparseURL(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, err
	}
	if u.Scheme != "redis" && u.Scheme != "rediss" {
		return nil, fmt.Errorf("scheme must be redis or rediss, got %q", u.Scheme)
	}
	if u.Host == "" {
		return nil, fmt.Errorf("host is required")
	}
	if p := strings.TrimPrefix(u.Path, "/"); p != "" {
		n, err := strconv.Atoi(p)
		if err != nil || n < 0 {
			return nil, fmt.Errorf("db index %q must be a non-negative integer", p)
		}
	}
	return u, nil
}

// storeDriverOrDefault returns the store driver name, defaulting to "memory" when empty.
func storeDriverOrDefault(d string) string {
	if d == "" {
		return "memory"
	}
	return d
}

// isValidPGIdentifier returns true when s is a valid PostgreSQL identifier:
// non-empty, at most 63 bytes, starts with letter or underscore, and contains
// only letters, digits, and underscores.
func isValidPGIdentifier(s string) bool {
	if s == "" || len(s) > 63 {
		return false
	}
	for i, r := range s {
		switch {
		case r == '_':
		case r >= 'a' && r <= 'z':
		case r >= 'A' && r <= 'Z':
		case r >= '0' && r <= '9' && i > 0:
		default:
			return false
		}
	}
	return true
}

// validateFeedAuth checks one feed-sink auth block. isOverride marks a
// per-surface block (which, when present, must define a method or be disabled);
// the top-level default may legitimately be empty (public).
func validateFeedAuth(i int, name, label string, a FeedAuthConfig, isOverride bool) error {
	if a.Disabled && a.HasMethods() {
		return fmt.Errorf("sinks[%d] (feed %q): %s.disabled cannot be combined with credentials", i, name, label)
	}
	if isOverride && !a.Disabled && !a.HasMethods() {
		return fmt.Errorf("sinks[%d] (feed %q): %s defines no credentials and is not disabled (set disabled: true for a public surface)", i, name, label)
	}
	var basicNames, bearerNames, apiNames []string
	for _, b := range a.BasicUsers {
		if b.Username == "" || b.Password == "" {
			return fmt.Errorf("sinks[%d] (feed %q): %s.basic_users entries need both username and password", i, name, label)
		}
		basicNames = append(basicNames, b.Name)
	}
	for _, t := range a.BearerTokens {
		if t.Token == "" {
			return fmt.Errorf("sinks[%d] (feed %q): %s.bearer_tokens entries need a token", i, name, label)
		}
		bearerNames = append(bearerNames, t.Name)
	}
	for _, k := range a.APIKeys {
		if k.Key == "" {
			return fmt.Errorf("sinks[%d] (feed %q): %s.api_keys entries need a key", i, name, label)
		}
		apiNames = append(apiNames, k.Name)
	}
	for _, set := range []struct {
		kind  string
		names []string
	}{{"basic_users", basicNames}, {"bearer_tokens", bearerNames}, {"api_keys", apiNames}} {
		if dup := firstDupFeedName(set.names); dup != "" {
			return fmt.Errorf("sinks[%d] (feed %q): %s.%s has duplicate name %q", i, name, label, set.kind, dup)
		}
	}
	if h := a.APIKeyHeader; h != "" {
		if strings.ContainsAny(h, " \t:") {
			return fmt.Errorf("sinks[%d] (feed %q): %s.api_key_header %q is not a valid header name", i, name, label, h)
		}
		if len(a.APIKeys) == 0 {
			return fmt.Errorf("sinks[%d] (feed %q): %s.api_key_header requires at least one api_keys entry", i, name, label)
		}
	}
	return nil
}

// firstDupFeedName returns the first duplicated non-empty name, or "".
func firstDupFeedName(names []string) string {
	seen := map[string]bool{}
	for _, n := range names {
		if n == "" {
			continue // unnamed credentials never collide
		}
		if seen[n] {
			return n
		}
		seen[n] = true
	}
	return ""
}

// validateTrustedProxyEntry accepts a preset token (private, all) or a CIDR /
// bare IP. Kept in lockstep with the runtime parser in internal/sink/feed.
func validateTrustedProxyEntry(raw string) error {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "":
		return fmt.Errorf("must not be empty")
	case "private", "all":
		return nil
	}
	if _, _, err := net.ParseCIDR(strings.TrimSpace(raw)); err == nil {
		return nil
	}
	if net.ParseIP(strings.TrimSpace(raw)) != nil {
		return nil
	}
	return fmt.Errorf("not a preset (private|all), CIDR, or IP")
}

// validateRetry checks that a RetryConfig is self-consistent. label is the
// config path (e.g. "http.retry" or "feeds[0].http.retry") used in error messages.
func validateRetry(label string, r RetryConfig) error {
	if r.MaxAttempts < 0 {
		return fmt.Errorf("%s.max_attempts must not be negative", label)
	}
	if r.BaseDelay < 0 {
		return fmt.Errorf("%s.base_delay must not be negative", label)
	}
	if r.MaxDelay < 0 {
		return fmt.Errorf("%s.max_delay must not be negative", label)
	}
	if r.MaxDelay != 0 && r.BaseDelay != 0 && r.MaxDelay < r.BaseDelay {
		return fmt.Errorf("%s.max_delay %v is below base_delay %v", label, r.MaxDelay, r.BaseDelay)
	}
	return nil
}

// pgSSLModeIsDisable returns true when dsn explicitly sets sslmode=disable in
// either the URL query (`postgres://...?sslmode=disable`) or the keyword form
// (`host=... sslmode=disable`). Conservative — anything we can't unambiguously
// classify as disable returns false (so we don't reject valid configs).
func pgSSLModeIsDisable(dsn string) bool {
	// URL form
	if u, err := url.Parse(dsn); err == nil && (u.Scheme == "postgres" || u.Scheme == "postgresql") {
		if v := u.Query().Get("sslmode"); v != "" {
			return strings.EqualFold(v, "disable")
		}
	}
	// Keyword form: tolerate optional whitespace around `=` (pgx itself does).
	if m := pgKeywordSSLModeRE.FindStringSubmatch(dsn); m != nil {
		val := strings.Trim(strings.ToLower(m[1]), `"'`)
		return val == "disable"
	}
	return false
}
