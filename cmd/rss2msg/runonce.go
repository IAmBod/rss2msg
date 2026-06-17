package main

import (
	"context"

	"github.com/spf13/cobra"

	"github.com/iambod/rss2msg/internal/scheduler"
)

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
