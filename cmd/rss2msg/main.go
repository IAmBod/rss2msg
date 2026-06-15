package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/iambod/rss2msg/internal/config"
	"github.com/iambod/rss2msg/internal/feedsource"
	"github.com/iambod/rss2msg/internal/health"
	"github.com/iambod/rss2msg/internal/scheduler"
	"github.com/iambod/rss2msg/internal/telemetry"
)

type rootOpts struct {
	configPath string
}

func newRootCmd() *cobra.Command {
	opts := &rootOpts{}
	root := &cobra.Command{
		Use:           "rss2msg",
		Short:         "Poll RSS/Atom feeds and publish changes to configured sinks",
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	root.PersistentFlags().StringVar(&opts.configPath, "config", "", "Path to config file (default: ./config.yaml or /etc/rss2msg/config.yaml)")
	root.AddCommand(newServeCmd(opts), newRunOnceCmd(opts), newLambdaCmd(opts), newAzureFunctionsCmd(opts), newValidateConfigCmd(opts), newGenerateConfigCmd(), newHealthcheckCmd(opts), newVersionCmd())
	return root
}

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
			var k8sSources []*feedsource.Kubernetes
			for _, src := range sources {
				if ks, ok := src.(*feedsource.Kubernetes); ok {
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

func newRunOnceCmd(opts *rootOpts) *cobra.Command {
	return &cobra.Command{
		Use:   "run-once",
		Short: "Poll every feed once and exit",
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
			pipes, err := buildOneShotPipelines(ctx, cfg, w)
			if err != nil {
				return err
			}
			return scheduler.RunOnce(ctx, scheduler.RunOnceConfig{
				Pipelines:   pipes,
				Concurrency: cfg.Runtime.RunOnceConcurrency,
			})
		},
	}
}

func newValidateConfigCmd(opts *rootOpts) *cobra.Command {
	return &cobra.Command{
		Use:   "validate-config",
		Short: "Parse config and verify state store and sinks are reachable",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			cfg, err := config.Load(opts.configPath)
			if err != nil {
				return err
			}
			warnings, err := config.Validate(cfg)
			for _, w := range warnings {
				fmt.Fprintln(os.Stderr, "warning:", w)
			}
			if err != nil {
				return err
			}
			tel, err := telemetry.Setup(ctx, cfg, os.Stderr)
			if err != nil {
				return err
			}
			defer func() { _ = tel.Shutdown(context.Background()) }()
			w, err := wireAll(ctx, cfg, tel)
			if err != nil {
				return err
			}
			defer w.Close()

			if err := w.store.Ping(ctx); err != nil {
				return fmt.Errorf("state store: %w", err)
			}
			fmt.Fprintln(os.Stdout, "config OK")
			return nil
		},
	}
}

func bootstrap(ctx context.Context, opts *rootOpts) (config.Config, *telemetry.Telemetry, *wired, error) {
	cfg, err := config.Load(opts.configPath)
	if err != nil {
		return config.Config{}, nil, nil, err
	}
	warnings, err := config.Validate(cfg)
	for _, w := range warnings {
		fmt.Fprintln(os.Stderr, "warning:", w)
	}
	if err != nil {
		return config.Config{}, nil, nil, err
	}
	tel, err := telemetry.Setup(ctx, cfg, os.Stderr)
	if err != nil {
		return config.Config{}, nil, nil, err
	}
	w, err := wireAll(ctx, cfg, tel)
	if err != nil {
		_ = tel.Shutdown(context.Background())
		return config.Config{}, nil, nil, err
	}
	return cfg, tel, w, nil
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Report unrecovered panics to Sentry (if enabled by config) before the
	// process dies, then re-panic so the default crash behaviour is unchanged.
	defer func() {
		if r := recover(); r != nil {
			reportPanic(r)
			panic(r)
		}
	}()

	root := newRootCmd()
	// Inside a serverless host with no explicit subcommand, default to the
	// matching handler (lambda or azure-functions) so a bare binary — a zip
	// custom-runtime bootstrap, an Azure Functions custom handler, or a
	// command-less image — self-starts.
	root.SetArgs(effectiveArgs(os.Args, implicitSubcommand(os.Getenv))[1:])
	if err := root.ExecuteContext(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
