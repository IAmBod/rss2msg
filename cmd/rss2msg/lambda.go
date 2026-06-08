package main

import (
	"context"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/spf13/cobra"

	"github.com/iambod/rss2msg/internal/scheduler"
)

// invokeEvent is the (optional) Lambda invocation payload. An EventBridge
// Scheduler trigger sends an opaque JSON object, so every field is optional and
// the zero value is a valid "poll everything with the configured defaults"
// request. Concurrency, when > 0, overrides runtime.run_once_concurrency for
// this single invocation.
type invokeEvent struct {
	Concurrency int `json:"concurrency,omitempty"`
}

// invokeResult is returned to the Lambda runtime. It is surfaced in the invoke
// response and in CloudWatch logs, giving a compact summary of the poll cycle.
type invokeResult struct {
	Feeds int    `json:"feeds"`
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}

// pollHandler returns a Lambda handler that runs exactly one poll-detect-publish
// cycle across every pipeline per invocation. The expensive wiring (config,
// state store, coordinator, sink connections) is built once at cold start by the
// caller and closed over here, so warm invocations reuse the same connections.
//
// Errors are returned to the runtime so the invocation is marked failed (and is
// retried per the trigger's policy); the same summary is also embedded in the
// result for callers that inspect the response payload.
func pollHandler(pipes []scheduler.FeedPipeline, defaultConcurrency int) func(context.Context, invokeEvent) (invokeResult, error) {
	return func(ctx context.Context, ev invokeEvent) (invokeResult, error) {
		concurrency := defaultConcurrency
		if ev.Concurrency > 0 {
			concurrency = ev.Concurrency
		}
		err := scheduler.RunOnce(ctx, scheduler.RunOnceConfig{
			Pipelines:   pipes,
			Concurrency: concurrency,
		})
		res := invokeResult{Feeds: len(pipes), OK: err == nil}
		if err != nil {
			res.Error = err.Error()
		}
		return res, err
	}
}

// effectiveArgs injects an implicit subcommand (resolved by implicitSubcommand
// from the serverless execution environment) when no explicit subcommand was
// given. This lets a bare binary — a zip custom-runtime `bootstrap`, an Azure
// Functions custom handler, or a container with no command — start the right
// handler automatically, while any explicit subcommand is left untouched.
func effectiveArgs(args []string, implicit string) []string {
	if implicit == "" || len(args) > 1 {
		return args
	}
	return append(args, implicit)
}

// newLambdaCmd is the entry point used inside the AWS Lambda execution
// environment. It performs the same cold-start wiring as `run-once`, then hands
// control to the Lambda runtime loop, which invokes the handler once per event.
func newLambdaCmd(opts *rootOpts) *cobra.Command {
	return &cobra.Command{
		Use:   "lambda",
		Short: "Run as an AWS Lambda function: one poll cycle per invocation",
		Long: `Run rss2msg inside the AWS Lambda runtime.

Cold-start wiring (config, state store, coordinator, and sink connections) is
done once when the execution environment starts, then the Lambda runtime invokes
the poll cycle once per event. Trigger it on a schedule with EventBridge
Scheduler.

Lambda's filesystem is ephemeral and invocations run concurrently, so use an
external state store (postgres or dynamodb) and a distributed coordinator
(postgres, redis, or dynamodb) — never sqlite state or the memory coordinator.`,
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

			// StartWithOptions returns when ctx is cancelled (SIGTERM on
			// shutdown), letting the deferred cleanup run.
			lambda.StartWithOptions(pollHandler(pipes, cfg.Runtime.RunOnceConcurrency), lambda.WithContext(ctx))
			return nil
		},
	}
}
