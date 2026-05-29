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
	"postgres": {},
	"kafka":    {},
	"rabbitmq": {},
	"sqs":      {},
	"sns":      {},
	"stdout":   {},
	"http":     {},
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

// Validate enforces the startup rules from the spec.
func Validate(c Config) error {
	if _, ok := knownStateDrivers[c.State.Driver]; !ok {
		return fmt.Errorf("state.driver %q is not supported", c.State.Driver)
	}
	switch c.State.Driver {
	case "postgres":
		dsn := strings.TrimSpace(c.State.Postgres.DSN)
		if dsn == "" {
			return fmt.Errorf("state.postgres.dsn is required when state.driver=postgres")
		}
		stls := c.State.Postgres.TLS
		tlsConfigured := stls.CAFile != "" || stls.CertFile != "" || stls.KeyFile != "" ||
			stls.ServerName != "" || stls.InsecureSkipVerify
		if tlsConfigured && pgSSLModeIsDisable(dsn) {
			return fmt.Errorf("state.postgres.tls is set but the DSN has sslmode=disable; remove sslmode=disable or drop the tls block")
		}
		if (stls.CertFile == "") != (stls.KeyFile == "") {
			return fmt.Errorf("state.postgres.tls.cert_file and key_file must both be set or both empty")
		}
	case "sqlite":
		if strings.TrimSpace(c.State.SQLite.Path) == "" {
			return fmt.Errorf("state.sqlite.path is required when state.driver=sqlite")
		}
	}

	if _, ok := knownCoordinationDrivers[c.Coordination.Driver]; !ok {
		return fmt.Errorf("coordination.driver %q is not supported", c.Coordination.Driver)
	}
	if c.Coordination.Driver == "postgres" {
		dsn := strings.TrimSpace(c.Coordination.Postgres.DSN)
		if dsn == "" {
			dsn = strings.TrimSpace(c.State.Postgres.DSN)
		}
		if dsn == "" {
			return fmt.Errorf("coordination.postgres.dsn (or state.postgres.dsn fallback) is required when coordination.driver=postgres")
		}
		tls := c.Coordination.Postgres.TLS
		tlsConfigured := tls.CAFile != "" || tls.CertFile != "" || tls.KeyFile != "" ||
			tls.ServerName != "" || tls.InsecureSkipVerify
		if tlsConfigured {
			if pgSSLModeIsDisable(dsn) {
				return fmt.Errorf("coordination.postgres.tls is set but the DSN has sslmode=disable; remove sslmode=disable or drop the tls block")
			}
		}
		if (tls.CertFile == "") != (tls.KeyFile == "") {
			return fmt.Errorf("coordination.postgres.tls.cert_file and key_file must both be set or both empty")
		}
	}
	if c.Coordination.Driver == "redis" {
		raw := strings.TrimSpace(c.Coordination.Redis.URL)
		if raw == "" {
			return fmt.Errorf("coordination.redis.url is required when coordination.driver=redis")
		}
		if _, err := redisparseURL(raw); err != nil {
			// Best-effort redact credentials before embedding the URL in the error.
			safe := raw
			if u, perr := url.Parse(raw); perr == nil {
				safe = u.Redacted()
			}
			return fmt.Errorf("coordination.redis.url %q is not parseable: %w", safe, err)
		}
		if ttl := c.Coordination.Redis.LockTTL; ttl != 0 && ttl < time.Second {
			return fmt.Errorf("coordination.redis.lock_ttl %v is below the 1s minimum", ttl)
		}
		if ri := c.Coordination.Redis.RenewalInterval; ri != 0 {
			ttl := c.Coordination.Redis.LockTTL
			if ttl == 0 {
				ttl = 30 * time.Second
			}
			if ri >= ttl {
				return fmt.Errorf("coordination.redis.renewal_interval %v must be less than lock_ttl %v", ri, ttl)
			}
		}
		tls := c.Coordination.Redis.TLS
		tlsConfigured := tls.CAFile != "" || tls.CertFile != "" || tls.KeyFile != "" ||
			tls.ServerName != "" || tls.InsecureSkipVerify
		if tlsConfigured {
			u, _ := url.Parse(strings.TrimSpace(c.Coordination.Redis.URL))
			if u == nil || u.Scheme != "rediss" {
				return fmt.Errorf("coordination.redis.tls is only valid when coordination.redis.url uses the rediss:// scheme")
			}
		}
		if (tls.CertFile == "") != (tls.KeyFile == "") {
			return fmt.Errorf("coordination.redis.tls.cert_file and key_file must both be set or both empty")
		}
	}

	if len(c.Sinks) == 0 {
		return fmt.Errorf("at least one sink must be declared")
	}
	names := make(map[string]struct{}, len(c.Sinks))
	for i, s := range c.Sinks {
		if strings.TrimSpace(s.Name) == "" {
			return fmt.Errorf("sinks[%d].name is required", i)
		}
		if _, dup := names[s.Name]; dup {
			return fmt.Errorf("duplicate sink name %q", s.Name)
		}
		names[s.Name] = struct{}{}
		if _, ok := knownSinkDrivers[s.Driver]; !ok {
			return fmt.Errorf("sinks[%d].driver %q is not supported", i, s.Driver)
		}
	}
	for i, s := range c.Sinks {
		if s.DeadLetter != "" {
			if s.DeadLetter == s.Name {
				return fmt.Errorf("sinks[%d].dead_letter must not refer to its own sink %q", i, s.Name)
			}
			if _, ok := names[s.DeadLetter]; !ok {
				return fmt.Errorf("sinks[%d].dead_letter %q does not refer to a declared sink", i, s.DeadLetter)
			}
		}
		switch s.Driver {
		case "sqs":
			if strings.TrimSpace(s.SQS.QueueURL) == "" {
				return fmt.Errorf("sinks[%d] (sqs %q): sqs.queue_url is required", i, s.Name)
			}
			fifo := strings.HasSuffix(s.SQS.QueueURL, ".fifo")
			if mg := s.SQS.MessageGroup; mg != "" {
				if _, ok := knownSQSMessageGroups[mg]; !ok {
					return fmt.Errorf("sinks[%d] (sqs %q): unknown message_group %q (want one of feed_url, item_id, sink)", i, s.Name, mg)
				}
				if !fifo {
					return fmt.Errorf("sinks[%d] (sqs %q): message_group is only valid for FIFO queues (queue_url must end with .fifo)", i, s.Name)
				}
			}
		case "sns":
			if strings.TrimSpace(s.SNS.TopicARN) == "" {
				return fmt.Errorf("sinks[%d] (sns %q): sns.topic_arn is required", i, s.Name)
			}
			fifo := strings.HasSuffix(s.SNS.TopicARN, ".fifo")
			if mg := s.SNS.MessageGroup; mg != "" {
				if _, ok := knownSQSMessageGroups[mg]; !ok {
					return fmt.Errorf("sinks[%d] (sns %q): unknown message_group %q (want one of feed_url, item_id, sink)", i, s.Name, mg)
				}
				if !fifo {
					return fmt.Errorf("sinks[%d] (sns %q): message_group is only valid for FIFO topics (topic_arn must end with .fifo)", i, s.Name)
				}
			}
		case "stdout":
			if _, ok := knownStdoutTargets[s.Stdout.Target]; !ok {
				return fmt.Errorf("sinks[%d] (stdout %q): unknown target %q (want stdout or stderr)", i, s.Name, s.Stdout.Target)
			}
			if _, ok := knownStdoutFormats[s.Stdout.Format]; !ok {
				return fmt.Errorf("sinks[%d] (stdout %q): unknown format %q (want json or pretty)", i, s.Name, s.Stdout.Format)
			}
		case "http":
			raw := strings.TrimSpace(s.HTTP.URL)
			if raw == "" {
				return fmt.Errorf("sinks[%d] (http %q): http.url is required", i, s.Name)
			}
			u, err := url.Parse(raw)
			if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
				return fmt.Errorf("sinks[%d] (http %q): http.url %q is not a valid http(s) URL", i, s.Name, raw)
			}
			if _, ok := knownHTTPSinkMethods[s.HTTP.Method]; !ok {
				return fmt.Errorf("sinks[%d] (http %q): unknown method %q (want POST or PUT)", i, s.Name, s.HTTP.Method)
			}
			for _, c := range s.HTTP.SuccessCodes {
				if c < 100 || c > 599 {
					return fmt.Errorf("sinks[%d] (http %q): success_code %d is out of range 100-599", i, s.Name, c)
				}
			}
		case "rabbitmq":
			if strings.TrimSpace(s.RabbitMQ.URL) == "" {
				return fmt.Errorf("sinks[%d] (rabbitmq %q): rabbitmq.url is required", i, s.Name)
			}
			if et := s.RabbitMQ.ExchangeType; et != "" {
				if _, ok := knownRabbitMQExchangeTypes[et]; !ok {
					return fmt.Errorf("sinks[%d] (rabbitmq %q): unknown exchange_type %q (want one of direct, topic, fanout, headers)", i, s.Name, et)
				}
			}
			if s.RabbitMQ.Declare && strings.TrimSpace(s.RabbitMQ.Exchange) == "" {
				return fmt.Errorf("sinks[%d] (rabbitmq %q): declare=true requires a non-empty exchange (the default exchange cannot be declared)", i, s.Name)
			}
		}
	}

	hasDefault := false
	if _, ok := names["default"]; ok {
		hasDefault = true
	}

	if len(c.Feeds) == 0 {
		return fmt.Errorf("at least one feed must be declared")
	}
	for i, f := range c.Feeds {
		if strings.TrimSpace(f.URL) == "" {
			return fmt.Errorf("feeds[%d].url is required", i)
		}
		if f.Interval < time.Second {
			return fmt.Errorf("feeds[%d].interval %v is below the 1s minimum", i, f.Interval)
		}
		if len(f.Sinks) == 0 {
			if !hasDefault {
				return fmt.Errorf(`feeds[%d] has no sinks and no sink named "default" is declared`, i)
			}
		}
		for _, name := range f.Sinks {
			if _, ok := names[name]; !ok {
				return fmt.Errorf("feeds[%d].sinks references unknown sink %q", i, name)
			}
		}
		for h := range f.HTTP.Headers {
			canon := textproto.CanonicalMIMEHeaderKey(h)
			if _, bad := reservedHeaders[canon]; bad {
				return fmt.Errorf("feeds[%d].http.headers must not set reserved cache header %q", i, h)
			}
		}
	}
	return nil
}

// ResolveFeedSinks returns the sink names a feed publishes to, applying the
// "no sinks -> default" fallback.
func ResolveFeedSinks(f FeedConfig) []string {
	if len(f.Sinks) > 0 {
		return f.Sinks
	}
	return []string{"default"}
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
