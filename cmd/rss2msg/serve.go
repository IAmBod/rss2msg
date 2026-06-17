package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/iambod/rss2msg/internal/feedsource"
	k8ssource "github.com/iambod/rss2msg/internal/feedsource/sources/kubernetes"
	"github.com/iambod/rss2msg/internal/health"
	"github.com/iambod/rss2msg/internal/scheduler"
)

func newServeCmd(opts *rootOpts) *cobra.Command {
	return &cobra.Command{
		Use:   "serve",
		Short: "Run as a long-lived daemon, one goroutine per feed",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			cfg, tel, w, err := bootstrap(ctx, opts)
			if err != nil {
				return err
			}
			defer func() {
				_ = tel.Shutdown(context.Background())
				w.Close()
			}()

			sources, closeSources, err := buildSources(cfg)
			if err != nil {
				return err
			}
			defer closeSources()

			agg := feedsource.NewAggregator(sources...)

			// Collect kubernetes sources so poll outcomes can be written back to
			// their Feed CR .status. ReportPoll is a no-op for feeds a source does
			// not own, so fanning out to all of them is safe. Only the replica that
			// won the per-feed lease polls the feed, so there is no status write
			// contention across replicas.
			var k8sSources []*k8ssource.Kubernetes
			for _, src := range sources {
				if ks, ok := src.(*k8ssource.Kubernetes); ok {
					k8sSources = append(k8sSources, ks)
				}
			}

			readyChecks := []health.Check{
				{Name: "state", Fn: w.store.Ping},
			}
			if p, ok := w.coord.(health.Pinger); ok {
				readyChecks = append(readyChecks, health.Check{Name: "coordination", Fn: p.Ping})
			}
			hs := health.New(cfg.Health, tel.Logger, readyChecks...)
			if err := hs.Start(); err != nil {
				return err
			}
			defer func() {
				sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				_ = hs.Shutdown(sctx)
			}()
			hs.MarkStarted()
			go func() {
				<-ctx.Done()
				hs.SetDraining()
			}()

			// SIGHUP forces a re-read of all sources.
			hup := make(chan os.Signal, 1)
			signal.Notify(hup, syscall.SIGHUP)
			defer signal.Stop(hup)
			go func() {
				for {
					select {
					case <-ctx.Done():
						return
					case <-hup:
						tel.Logger.Info().Msg("SIGHUP: reloading feed sources")
						agg.Trigger()
					}
				}
			}()

			return scheduler.ServeDynamic(ctx, scheduler.DynamicConfig{
				Provider:     agg,
				Factory:      w.factory,
				DrainTimeout: cfg.Runtime.ShutdownDrainTimeout,
				OnReconcile: func(added, removed, changed int) {
					if added+removed+changed > 0 {
						tel.Logger.Info().
							Int("added", added).Int("removed", removed).Int("changed", changed).
							Msg("feeds reconciled")
					}
				},
				OnError: func(err error) {
					tel.Logger.Error().Err(err).Msg("feed reconcile aborted")
				},
				OnPollOverrun: func(feedURL string, took, interval time.Duration) {
					w.instr.PollOverran.Add(ctx, 1, metric.WithAttributes(attribute.String("feed_url", feedURL)))
					tel.Logger.Warn().
						Str("feed_url", feedURL).
						Dur("took", took).
						Dur("interval", interval).
						Msg("poll overran its interval; effective polling rate is below configured")
				},
				OnPollComplete: func(feedURL string, changeCount int, err error, when time.Time) {
					for _, ks := range k8sSources {
						ks.ReportPoll(ctx, feedURL, changeCount, err, when)
					}
				},
			})
		},
	}
}
