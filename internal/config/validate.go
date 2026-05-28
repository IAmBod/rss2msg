package config

import (
	"fmt"
	"net/textproto"
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
		if s.DeadLetter == "" {
			continue
		}
		if s.DeadLetter == s.Name {
			return fmt.Errorf("sinks[%d].dead_letter must not refer to its own sink %q", i, s.Name)
		}
		if _, ok := names[s.DeadLetter]; !ok {
			return fmt.Errorf("sinks[%d].dead_letter %q does not refer to a declared sink", i, s.DeadLetter)
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
