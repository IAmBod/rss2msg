package main

import (
	"context"
	"fmt"

	"github.com/iambod/rss2msg/internal/config"
	"github.com/iambod/rss2msg/internal/feedsource"
	filesource "github.com/iambod/rss2msg/internal/feedsource/sources/file"
	httpsource "github.com/iambod/rss2msg/internal/feedsource/sources/http"
	k8ssource "github.com/iambod/rss2msg/internal/feedsource/sources/kubernetes"
	pgsource "github.com/iambod/rss2msg/internal/feedsource/sources/postgres"
	"github.com/iambod/rss2msg/internal/scheduler"
)

// buildOneShotPipelines resolves the feed list for the one-shot execution modes
// (run-once, lambda) and builds a pipeline per feed via the wired factory. It
// reads cfg.Feeds AND any configured feed_sources (file, postgres) exactly once
// through buildSources + feedsource.Snapshot — the same resolution serve uses —
// so dynamic feed sources work outside the long-lived daemon. The source
// connections are closed once the snapshot is taken; the pipelines reference the
// long-lived store/coordinator/sinks owned by wired, not the sources.
func buildOneShotPipelines(ctx context.Context, cfg config.Config, w *wired) ([]scheduler.FeedPipeline, error) {
	sources, closeSources, err := buildSources(cfg)
	if err != nil {
		return nil, err
	}
	defer closeSources()
	feeds, err := feedsource.Snapshot(ctx, sources...)
	if err != nil {
		return nil, err
	}
	pipes := make([]scheduler.FeedPipeline, 0, len(feeds))
	for _, fc := range feeds {
		p, err := w.factory(fc)
		if err != nil {
			return nil, err
		}
		pipes = append(pipes, p)
	}
	return pipes, nil
}

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
			f, err := filesource.NewFile(name, sc.Path)
			if err != nil {
				closeAll(closers)
				return nil, nil, err
			}
			closers = append(closers, func() { _ = f.Close() })
			sources = append(sources, f)
		case "postgres":
			opts := pgsource.PostgresOptions{
				Name:     name,
				DSN:      sc.Postgres.DSN,
				Table:    sc.Postgres.Table,
				Query:    sc.Postgres.Query,
				Interval: sc.Interval,
			}
			if sc.Postgres.TLS != (config.FeedSourcePGTLSConfig{}) {
				opts.TLS = &pgsource.PostgresTLSOptions{
					CAFile:             sc.Postgres.TLS.CAFile,
					CertFile:           sc.Postgres.TLS.CertFile,
					KeyFile:            sc.Postgres.TLS.KeyFile,
					ServerName:         sc.Postgres.TLS.ServerName,
					InsecureSkipVerify: sc.Postgres.TLS.InsecureSkipVerify,
				}
			}
			p, err := pgsource.NewPostgres(context.Background(), opts)
			if err != nil {
				closeAll(closers)
				return nil, nil, err
			}
			closers = append(closers, func() { _ = p.Close() })
			sources = append(sources, p)
		case "http":
			opts := httpsource.HTTPOptions{
				Name:     name,
				URL:      sc.HTTP.URL,
				Timeout:  sc.HTTP.Timeout,
				Headers:  sc.HTTP.Headers,
				Interval: sc.Interval,
			}
			if sc.HTTP.TLS != (config.FeedSourceHTTPTLSConfig{}) {
				opts.TLS = &httpsource.HTTPTLSOptions{
					CAFile:             sc.HTTP.TLS.CAFile,
					CertFile:           sc.HTTP.TLS.CertFile,
					KeyFile:            sc.HTTP.TLS.KeyFile,
					ServerName:         sc.HTTP.TLS.ServerName,
					InsecureSkipVerify: sc.HTTP.TLS.InsecureSkipVerify,
				}
			}
			h, err := httpsource.NewHTTP(opts)
			if err != nil {
				closeAll(closers)
				return nil, nil, err
			}
			closers = append(closers, func() { _ = h.Close() })
			sources = append(sources, h)
		case "kubernetes":
			writeStatus := true
			if sc.Kubernetes.WriteStatus != nil {
				writeStatus = *sc.Kubernetes.WriteStatus
			}
			k, err := k8ssource.NewKubernetes(context.Background(), k8ssource.KubernetesOptions{
				Name:           name,
				Namespace:      sc.Kubernetes.Namespace,
				LabelSelector:  sc.Kubernetes.LabelSelector,
				ResyncInterval: sc.Kubernetes.ResyncInterval,
				WriteStatus:    writeStatus,
			}, sc.Kubernetes.Kubeconfig)
			if err != nil {
				closeAll(closers)
				return nil, nil, err
			}
			closers = append(closers, func() { _ = k.Close() })
			sources = append(sources, k)
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
