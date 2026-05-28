package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/iambod/rss2msg/internal/config"
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
	root.AddCommand(newServeCmd(opts), newRunOnceCmd(opts), newValidateConfigCmd(opts))
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

			intervals := map[string]time.Duration{}
			pipes := make([]scheduler.FeedPipeline, 0, len(w.pipelines))
			for _, p := range w.pipelines {
				intervals[p.FeedURL()] = p.cfg.Interval
				pipes = append(pipes, p)
			}
			return scheduler.Serve(ctx, scheduler.ServeConfig{
				Pipelines:    pipes,
				Intervals:    intervals,
				DrainTimeout: cfg.Runtime.ShutdownDrainTimeout,
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
			pipes := make([]scheduler.FeedPipeline, 0, len(w.pipelines))
			for _, p := range w.pipelines {
				pipes = append(pipes, p)
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
			if err := config.Validate(cfg); err != nil {
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
	if err := config.Validate(cfg); err != nil {
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

	if err := newRootCmd().ExecuteContext(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
