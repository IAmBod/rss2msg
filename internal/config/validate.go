package config

import (
	"fmt"
	"net/textproto"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
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
}

var knownSinkDrivers = map[string]struct{}{
	"postgres":        {},
	"kafka":           {},
	"rabbitmq":        {},
	"sqs":             {},
	"sns":             {},
	"stdout":          {},
	"http":            {},
	"feed":            {},
	"composite":       {},
	"gcp_pubsub":      {},
	"azureservicebus": {},
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
			if _, ok := knownHTTPSinkMethods[s.HTTP.Method]; !ok {
				return *warnings, fmt.Errorf("sinks[%d] (http %q): unknown method %q (want POST or PUT)", i, s.Name, s.HTTP.Method)
			}
			for _, c := range s.HTTP.SuccessCodes {
				if c < 100 || c > 599 {
					return *warnings, fmt.Errorf("sinks[%d] (http %q): success_code %d is out of range 100-599", i, s.Name, c)
				}
			}
		case "rabbitmq":
			if strings.TrimSpace(s.RabbitMQ.URL) == "" {
				return *warnings, fmt.Errorf("sinks[%d] (rabbitmq %q): rabbitmq.url is required", i, s.Name)
			}
			if et := s.RabbitMQ.ExchangeType; et != "" {
				if _, ok := knownRabbitMQExchangeTypes[et]; !ok {
					return *warnings, fmt.Errorf("sinks[%d] (rabbitmq %q): unknown exchange_type %q (want one of direct, topic, fanout, headers)", i, s.Name, et)
				}
			}
			if s.RabbitMQ.Declare && strings.TrimSpace(s.RabbitMQ.Exchange) == "" {
				return *warnings, fmt.Errorf("sinks[%d] (rabbitmq %q): declare=true requires a non-empty exchange (the default exchange cannot be declared)", i, s.Name)
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
			rss := f.RSSPath
			if rss == "" {
				rss = "/rss"
			}
			atom := f.AtomPath
			if atom == "" {
				atom = "/atom"
			}
			if rss[0] != '/' || atom[0] != '/' || rss == atom {
				return *warnings, fmt.Errorf("sinks[%d] (feed %q): rss_path/atom_path must start with / and differ", i, s.Name)
			}
			for _, raw := range []string{f.Link, f.PublicURL} {
				if raw == "" {
					continue
				}
				if u, err := url.Parse(raw); err != nil || u.Scheme == "" || u.Host == "" {
					return *warnings, fmt.Errorf("sinks[%d] (feed %q): %q is not an absolute URL", i, s.Name, raw)
				}
			}
			if (f.TLS.CertFile == "") != (f.TLS.KeyFile == "") {
				return *warnings, fmt.Errorf("sinks[%d] (feed %q): tls.cert_file and key_file must both be set or both empty", i, s.Name)
			}
			hasBasic := f.Auth.Basic.Username != "" || f.Auth.Basic.Password != ""
			if hasBasic && f.Auth.BearerToken != "" {
				return *warnings, fmt.Errorf("sinks[%d] (feed %q): auth accepts only one of basic or bearer_token", i, s.Name)
			}
			if hasBasic && (f.Auth.Basic.Username == "" || f.Auth.Basic.Password == "") {
				return *warnings, fmt.Errorf("sinks[%d] (feed %q): auth.basic needs both username and password", i, s.Name)
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

	// Client-TLS blocks (postgres/kafka/rabbitmq/http sinks) require cert_file
	// and key_file to be set together for mutual-TLS.
	for i, s := range c.Sinks {
		var stls SinkTLSConfig
		switch s.Driver {
		case "postgres":
			stls = s.Postgres.TLS
		case "kafka":
			stls = s.Kafka.TLS
		case "rabbitmq":
			stls = s.RabbitMQ.TLS
		case "http":
			stls = s.HTTP.TLS
		default:
			continue
		}
		if (stls.CertFile == "") != (stls.KeyFile == "") {
			return *warnings, fmt.Errorf("sinks[%d] (%s %q): tls.cert_file and key_file must both be set or both empty", i, s.Driver, s.Name)
		}
	}

	hasDefault := false
	if _, ok := names["default"]; ok {
		hasDefault = true
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
	}
	for i, s := range c.FeedSources {
		switch s.Type {
		case "static":
			// no required fields; injects the feeds: block at this position
		case "file":
			if strings.TrimSpace(s.Path) == "" {
				return *warnings, fmt.Errorf("feed_sources[%d] (file): path is required", i)
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
