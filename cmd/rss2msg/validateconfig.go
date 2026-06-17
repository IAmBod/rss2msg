package main

import (
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/iambod/rss2msg/internal/config"
	"github.com/iambod/rss2msg/internal/telemetry"
)

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
