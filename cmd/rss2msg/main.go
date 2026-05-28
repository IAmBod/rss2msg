package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
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

func newServeCmd(_ *rootOpts) *cobra.Command {
	return &cobra.Command{
		Use:   "serve",
		Short: "Run as a long-lived daemon, one goroutine per feed",
		RunE: func(cmd *cobra.Command, args []string) error {
			return fmt.Errorf("serve: not implemented yet")
		},
	}
}

func newRunOnceCmd(_ *rootOpts) *cobra.Command {
	return &cobra.Command{
		Use:   "run-once",
		Short: "Poll every feed once and exit",
		RunE: func(cmd *cobra.Command, args []string) error {
			return fmt.Errorf("run-once: not implemented yet")
		},
	}
}

func newValidateConfigCmd(_ *rootOpts) *cobra.Command {
	return &cobra.Command{
		Use:   "validate-config",
		Short: "Parse config, dial state store and each sink, exit 0/1",
		RunE: func(cmd *cobra.Command, args []string) error {
			return fmt.Errorf("validate-config: not implemented yet")
		},
	}
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := newRootCmd().ExecuteContext(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
