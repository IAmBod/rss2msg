package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/iambod/rss2msg/internal/config"
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
