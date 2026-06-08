# Changelog

All notable changes to this project are documented here.
This file is generated from [Conventional Commits](https://www.conventionalcommits.org)
by [git-cliff](https://git-cliff.org).

## [0.1.0] - 2026-06-08

### Features

- **release:** Build .deb/.rpm/.apk packages via nFPM
- **sinks:** Add client TLS to postgres, kafka, rabbitmq, http sinks
- **release:** Publish a Homebrew cask via GoReleaser tap
- **sink:** Add Google Cloud Pub/Sub sink (gcp_pubsub)
- **cli:** Add generate-config subcommand
- **telemetry:** Add Graphite (Carbon) metrics exporter
- **telemetry:** Add optional Sentry error/crash reporting
- Add Helm chart for Kubernetes deployment
- **telemetry:** Optional PostHog log-event integration ([#79](https://github.com/IAmBod/rss2msg/issues/79))
- **feed-sink:** Reshape rss/atom into surface blocks, add mcp surface
- **feed-sink:** Validate feed surface paths (mcp, collisions, all-off)
- **feed-sink:** Carry surfaces into Options and gate routes by enabled
- **feed-sink:** Serve content tools over MCP at the mcp surface
- Add opt-in HTTP/3 (QUIC) support to http and feed sinks ([#76](https://github.com/IAmBod/rss2msg/issues/76))
- **telemetry:** Add CloudWatch config schema and validation
- **telemetry:** Ship logs to CloudWatch Logs via async batching hook
- **telemetry:** Export metrics to CloudWatch via PutMetricData
- **sink:** Add dapr_pubsub sink publishing to a Dapr sidecar
- **config:** Wire dapr_pubsub sink into config, validation, and builder
- **sink:** Add NATS sink (core + JetStream)
- **telemetry:** Add service.instance.id so multi-instance metrics don't collide
- **sink:** Add gRPC sink delivering Change to a ChangeSink server
- **config:** Warn when distributed coordinator pairs with sqlite state
- Add postgres feed source
- **runtime:** Per-sink deliver timeout and poll-overrun visibility
- **cli:** Add healthcheck subcommand for distroless Docker HEALTHCHECK
- Add dependabot auto-PR
- **sink:** Add Azure Cosmos DB (NoSQL) sink
- **state:** Add DynamoDB state store driver
- **coord:** Add DynamoDB coordinator (conditional-write lease)
- Add hot-path benchmarks and task bench target
- **sink:** Add DynamoDB sink driver
- **lambda:** Add native AWS Lambda handler subcommand
- **feeds:** Resolve feed_sources in run-once and lambda modes

### Bug Fixes

- **sinks:** Wire TLS config into buildPublisher
- **config:** Re-sync embedded example.yaml after merging gcp_pubsub block
- **telemetry:** Skip zero-count histograms in CloudWatch metrics

### Performance

- **feedsource:** Fetch sources concurrently in Aggregator.Desired

### Refactor

- **docker:** Merge Dockerfile.goreleaser into the production stage

### Documentation

- Add CMS platform feed-URL how-to ([#95](https://github.com/IAmBod/rss2msg/issues/95))
- Add platform deployment recipes ([#104](https://github.com/IAmBod/rss2msg/issues/104))
- Add AWS Lambda run-once deployment recipe ([#104](https://github.com/IAmBod/rss2msg/issues/104))
- **telemetry:** Document CloudWatch config, examples, and reference
- **claude:** Import AGENTS.md via @-include
- **sinks:** Document the dapr_pubsub sink
- **sinks:** Document the gRPC sink
- Document at-least-once delivery and feed-sink restart durability
- List all 14 sinks in README and document the health config block
- Correct healthcheck guidance in deploy docs
- Add OSS community-health files and populate changelog

### Testing

- **feed-sink:** MCP end-to-end test; docs(feed): document mcp surface
- **telemetry:** LocalStack integration for CloudWatch logs and metrics
- **sink:** Exercise dapr_pubsub against a real go-sdk client
- **cmd:** Cover pipeline.RunOnce orchestration paths
- **cmd:** Exercise run-once wiring end-to-end (no Docker)
- **cmd:** Cover the *TLSFromConfig mappers (0% -> 100%)
- **cmd:** Smoke-test the serve daemon (boot, poll, shutdown)
- **sink:** Cover Registry.Close/All and BranchState.String
- **feedsource:** Cover buildPostgresTLS (0% -> 100%)

### Build & CI

- Drop redundant go vet step from test job
- Lint and template the Helm chart
- Add benchstat benchmark regression gate on pull requests

### Dependencies

- **deps:** Bump github.com/dapr/dapr

### Miscellaneous

- Move config examples into examples/
- License rss2msg under the Business Source License 1.1
- **telemetry:** Satisfy linters and tidy CloudWatch deps
- **proto:** Add ChangeSink gRPC contract and buf codegen


