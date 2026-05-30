package main

import (
	"fmt"

	"github.com/iambod/rss2msg/internal/config"
	"github.com/iambod/rss2msg/internal/feedsource"
)

// buildSources constructs the ordered source list from config. If no
// feed_sources are configured, the static feeds: block is the sole source
// (preserving today's behavior). When feed_sources IS configured, a "static"
// entry injects the feeds: block at its position; otherwise the static block is
// not included.
func buildSources(cfg config.Config) ([]feedsource.Source, func(), error) {
	staticName := "static"
	if len(cfg.FeedSources) == 0 {
		return []feedsource.Source{feedsource.NewStatic(staticName, cfg.Feeds)}, func() {}, nil
	}

	var sources []feedsource.Source
	var closers []func()
	for i, sc := range cfg.FeedSources {
		name := sc.Name
		if name == "" {
			name = fmt.Sprintf("%s[%d]", sc.Type, i)
		}
		switch sc.Type {
		case "static":
			sources = append(sources, feedsource.NewStatic(name, cfg.Feeds))
		case "file":
			f, err := feedsource.NewFile(name, sc.Path)
			if err != nil {
				closeAll(closers)
				return nil, nil, err
			}
			closers = append(closers, func() { _ = f.Close() })
			sources = append(sources, f)
		default:
			closeAll(closers)
			return nil, nil, fmt.Errorf("feed_sources[%d]: unsupported type %q", i, sc.Type)
		}
	}
	return sources, func() { closeAll(closers) }, nil
}

func closeAll(fns []func()) {
	for _, fn := range fns {
		fn()
	}
}
