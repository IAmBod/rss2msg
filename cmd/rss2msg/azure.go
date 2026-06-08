package main

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/spf13/cobra"

	"github.com/iambod/rss2msg/internal/scheduler"
)

// implicitSubcommand detects a serverless execution environment from its
// signature environment variable and returns the subcommand the binary should
// auto-start. AWS Lambda sets AWS_LAMBDA_RUNTIME_API; an Azure Functions custom
// handler is launched by the Functions host with FUNCTIONS_CUSTOMHANDLER_PORT
// set. Lambda takes precedence if both are somehow present, so the Lambda
// runtime loop is entered rather than an HTTP server bound to the wrong port.
// Returns "" outside both environments, leaving the args untouched.
func implicitSubcommand(getenv func(string) string) string {
	if getenv("AWS_LAMBDA_RUNTIME_API") != "" {
		return "lambda"
	}
	if getenv("FUNCTIONS_CUSTOMHANDLER_PORT") != "" {
		return "azure-functions"
	}
	return ""
}

// customHandlerFunc returns an http.Handler that runs exactly one
// poll-detect-publish cycle across every pipeline per request. The Azure
// Functions host invokes a custom handler over HTTP — one request per trigger
// firing — so a timer trigger becomes one poll cycle. The expensive wiring
// (config, state store, coordinator, sink connections) is built once at cold
// start by the caller and closed over here, so warm invocations reuse the same
// connections.
//
// A failed cycle is reported as HTTP 500 so the Functions host marks the
// invocation failed (and retries per the trigger policy); the same compact
// summary is written to the response body and surfaced in the function logs.
func customHandlerFunc(pipes []scheduler.FeedPipeline, concurrency int) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		err := scheduler.RunOnce(r.Context(), scheduler.RunOnceConfig{
			Pipelines:   pipes,
			Concurrency: concurrency,
		})
		res := invokeResult{Feeds: len(pipes), OK: err == nil}
		if err != nil {
			res.Error = err.Error()
		}
		w.Header().Set("Content-Type", "application/json")
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
		}
		_ = json.NewEncoder(w).Encode(res)
	}
}

// newAzureFunctionsCmd is the entry point used inside the Azure Functions
// runtime as a custom handler. It performs the same cold-start wiring as
// `run-once`, then serves the Functions host over HTTP, running one poll cycle
// per invocation. The host launches the binary with FUNCTIONS_CUSTOMHANDLER_PORT
// set; the handler listens on 127.0.0.1 at that port. Inside Functions with no
// explicit subcommand the binary auto-starts this command (see implicitSubcommand).
func newAzureFunctionsCmd(opts *rootOpts) *cobra.Command {
	return &cobra.Command{
		Use:     "azure-functions",
		Aliases: []string{"azure"},
		Short:   "Run as an Azure Functions custom handler: one poll cycle per invocation",
		Long: `Run rss2msg as an Azure Functions custom handler.

Cold-start wiring (config, state store, coordinator, and sink connections) is
done once when the worker process starts, then the Functions host invokes the
poll cycle once per request — bind it to a timerTrigger to poll on a schedule.
The handler listens on 127.0.0.1 at the port the host provides via
FUNCTIONS_CUSTOMHANDLER_PORT (default 8080 when run outside the host).

A Functions worker's filesystem is ephemeral and invocations can run
concurrently across instances, so use an external state store (postgres) and a
distributed coordinator (postgres or redis) — never sqlite state or the memory
coordinator.`,
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

			// Resolve the feed list (cfg.Feeds + feed_sources) once at cold
			// start; warm invocations reuse this pipeline set and its
			// connections. Each cold start re-reads any dynamic feed_sources.
			pipes, err := buildOneShotPipelines(ctx, cfg, w)
			if err != nil {
				return err
			}

			port := os.Getenv("FUNCTIONS_CUSTOMHANDLER_PORT")
			if port == "" {
				port = "8080"
			}
			addr := net.JoinHostPort("127.0.0.1", port)

			// A single catch-all route handles every function the app defines
			// (the host POSTs to /<functionName>); each one runs one poll cycle.
			mux := http.NewServeMux()
			mux.HandleFunc("/", customHandlerFunc(pipes, cfg.Runtime.RunOnceConcurrency))
			srv := &http.Server{
				Addr:              addr,
				Handler:           mux,
				ReadHeaderTimeout: 5 * time.Second,
			}

			// Shut the server down when ctx is cancelled (SIGTERM on host
			// recycle), letting the deferred cleanup run.
			go func() {
				<-ctx.Done()
				sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				_ = srv.Shutdown(sctx)
			}()

			tel.Logger.Info().Str("addr", addr).Int("feeds", len(pipes)).
				Msg("azure functions custom handler listening")
			if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				return err
			}
			return nil
		},
	}
}
