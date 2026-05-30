// Package feedsource produces the desired feed list for the serve daemon from
// an ordered list of pluggable sources, merged by URL with earlier sources
// winning on collision.
package feedsource

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/iambod/rss2msg/internal/config"
)

// Source is one configured feed-source instance. Multiple instances of the
// same type may exist, each with its own config. Implementations must be safe
// for concurrent calls to Feeds and Changes.
type Source interface {
	// Name uniquely identifies this instance (used in logs/metrics).
	Name() string
	// Feeds returns this instance's current desired feed list.
	Feeds(ctx context.Context) ([]config.FeedConfig, error)
	// Changes signals that the caller should re-read Feeds. A nil/never-firing
	// channel is valid for sources whose contents cannot change at runtime.
	Changes() <-chan struct{}
}

// FeedSpec is the canonical wire/serialized shape a source yields per feed.
// Every external source (file, http, db, ...) decodes into FeedSpec, then
// ToFeedConfig converts to the internal config.FeedConfig.
type FeedSpec struct {
	URL      string        `json:"url" yaml:"url"`
	Interval string        `json:"interval,omitempty" yaml:"interval,omitempty"`
	Sinks    []string      `json:"sinks,omitempty" yaml:"sinks,omitempty"`
	HTTP     *FeedSpecHTTP `json:"http,omitempty" yaml:"http,omitempty"`
}

// FeedSpecHTTP mirrors config.FeedHTTPConfig in serialized form.
type FeedSpecHTTP struct {
	Timeout string            `json:"timeout,omitempty" yaml:"timeout,omitempty"`
	Headers map[string]string `json:"headers,omitempty" yaml:"headers,omitempty"`
}

// ToFeedConfig converts a FeedSpec to a config.FeedConfig, parsing durations.
// An empty URL is an error; sinks are left empty here (resolved to "default"
// downstream by config.ResolveFeedSinks).
func (s FeedSpec) ToFeedConfig() (config.FeedConfig, error) {
	if strings.TrimSpace(s.URL) == "" {
		return config.FeedConfig{}, fmt.Errorf("feed spec: url is required")
	}
	fc := config.FeedConfig{URL: s.URL, Sinks: s.Sinks}
	if s.Interval != "" {
		d, err := time.ParseDuration(s.Interval)
		if err != nil {
			return config.FeedConfig{}, fmt.Errorf("feed spec %s: interval: %w", s.URL, err)
		}
		fc.Interval = d
	}
	if s.HTTP != nil {
		fc.HTTP.Headers = s.HTTP.Headers
		if s.HTTP.Timeout != "" {
			d, err := time.ParseDuration(s.HTTP.Timeout)
			if err != nil {
				return config.FeedConfig{}, fmt.Errorf("feed spec %s: http.timeout: %w", s.URL, err)
			}
			fc.HTTP.Timeout = d
		}
	}
	return fc, nil
}

// SpecsToConfigs converts a slice of specs, failing on the first invalid one.
func SpecsToConfigs(specs []FeedSpec) ([]config.FeedConfig, error) {
	out := make([]config.FeedConfig, 0, len(specs))
	for _, s := range specs {
		fc, err := s.ToFeedConfig()
		if err != nil {
			return nil, err
		}
		out = append(out, fc)
	}
	return out, nil
}
