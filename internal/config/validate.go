package config

import (
	"fmt"
	"net/textproto"
	"net/url"
	"strconv"
	"strings"
	"time"
)

var reservedHeaders = map[string]struct{}{
	textproto.CanonicalMIMEHeaderKey("If-Modified-Since"): {},
	textproto.CanonicalMIMEHeaderKey("If-None-Match"):     {},
}

var knownStateDrivers = map[string]struct{}{
	"postgres": {},
}

var knownSinkDrivers = map[string]struct{}{
	"postgres": {},
	"kafka":    {},
	"rabbitmq": {},
	"sqs":      {},
	"sns":      {},
}

var knownCoordinationDrivers = map[string]struct{}{
	"":         {},
	"noop":     {},
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
		if strings.TrimSpace(c.State.Postgres.DSN) == "" {
			return fmt.Errorf("state.postgres.dsn is required when state.driver=postgres")
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
			if strings.HasSuffix(s.SQS.QueueURL, ".fifo") {
				return fmt.Errorf("sinks[%d] (sqs %q): FIFO queues are not supported in this version", i, s.Name)
			}
		case "sns":
			if strings.TrimSpace(s.SNS.TopicARN) == "" {
				return fmt.Errorf("sinks[%d] (sns %q): sns.topic_arn is required", i, s.Name)
			}
			if strings.HasSuffix(s.SNS.TopicARN, ".fifo") {
				return fmt.Errorf("sinks[%d] (sns %q): FIFO topics are not supported in this version", i, s.Name)
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
