# Changelog

All notable changes to this project are documented here.
This file is generated from [Conventional Commits](https://www.conventionalcommits.org)
by [git-cliff](https://git-cliff.org).

## [0.2.0] - 2026-06-15

### Features

- **coord:** Add Azure Cosmos DB lease coordinator
- **state:** Add Azure Cosmos DB state store
- **telemetry:** Add HTTP/protobuf OTLP transport for Grafana Cloud ([#63](https://github.com/IAmBod/rss2msg/issues/63))
- **azure:** Add native Azure Functions custom handler subcommand
- **kafka:** Add schema-registry encoder package with JSON Schema support
- **kafka:** Use schema-registry encoder for record value when configured
- **kafka:** Support registry TLS in schema encoder
- **config:** Add kafka schema_registry config block
- **config:** Validate kafka schema_registry block
- **kafka:** Wire schema_registry config into the sink
- **config:** Add HTTP feed source config structs
- **config:** Validate http feed source (url, mTLS pair, reserved headers)
- **feedsource:** Add http feed source with conditional GET
- **cmd:** Wire http feed source into buildSources
- **config:** Reject negative timeout on http feed source
- **kafka:** Add Avro schema-registry encoder
- **config:** Accept avro schema_registry format
- **kafka:** Add Protobuf schema-registry encoder
- **release:** Ship the user docs in archives and Linux packages
- **feedsource:** Map Feed CR unstructured object to FeedSpec
- **feedsource:** Kubernetes source watches Feed CRs via dynamic informer
- **feedsource:** NewKubernetes builds source from in-cluster/kubeconfig creds
- **config:** Kubernetes feed-source config + validation
- **cmd:** Wire kubernetes feed source into buildSources
- **scheduler:** OnPollComplete hook surfacing per-feed poll outcomes
- **scheduler:** OnPollComplete on DynamicConfig wired through startFeed
- **feedsource:** Write poll status back to Feed CR .status subresource
- **cmd:** Serve writes Feed CR status via OnPollComplete
- **deploy:** Feed CRD, RBAC, and Helm wiring for the kubernetes source

### Bug Fixes

- **feedsource:** Harden http source body read and transport defaults
- **feedsource:** Error on unsolicited 304 from http source
- **release:** Bundle the populated CHANGELOG.md into release artifacts
- **feedsource:** Evict stale URL index entry when a Feed CR url changes
- **feedsource:** Include message in Feed CR status Ready condition
- **deploy:** Default serviceAccount.automount=true for in-cluster source

### Refactor

- **kafka:** Tighten schema encoder validation per review
- **kafka:** Log registry insecure-TLS and cover validation branches

### Documentation

- **examples:** Add Docker Compose examples for common deployment scenarios
- Surface Helm in index, refresh stale v0.0.2 references
- Split CMS and external-integration guides into per-platform pages
- Split coordinators into per-backend pages
- Split state stores into a how-to hub + per-backend pages
- Rename how-to guides to task-oriented (Diátaxis) names
- **reference:** List feed_sources in the configuration reference
- **reference:** Add posthog to the telemetry config example
- **spec:** Grafana Cloud OTLP support design ([#63](https://github.com/IAmBod/rss2msg/issues/63))
- **grafana:** How-to, telemetry reference, and Alloy bridge example ([#63](https://github.com/IAmBod/rss2msg/issues/63))
- **spec:** Kafka Schema Registry support design ([#61](https://github.com/IAmBod/rss2msg/issues/61))
- **plan:** Kafka Schema Registry PR1 (JSON Schema) implementation plan ([#61](https://github.com/IAmBod/rss2msg/issues/61))
- **kafka:** Document optional schema_registry block
- **feedsource:** Design spec for HTTP feed source ([#161](https://github.com/IAmBod/rss2msg/issues/161))
- **feedsource:** Implementation plan for HTTP feed source ([#161](https://github.com/IAmBod/rss2msg/issues/161))
- Document http feed source
- Sync examples/config.example.yaml with embedded example (http source)
- **plan:** Kafka Schema Registry PR2 (Avro) implementation plan ([#61](https://github.com/IAmBod/rss2msg/issues/61))
- **kafka:** Document avro schema_registry format
- **kafka:** Document protobuf schema_registry format
- **plan:** Implementation plan for the K8s CRD feed source
- Document the kubernetes CRD feed source

### Testing

- **kafka:** Integration round-trip for JSON schema-registry encoding
- **kafka:** Cover auto_register=false, registration retry, and encode benchmark
- **feedsource:** Http source TLS round-trip and builder errors
- **kafka:** Integration round-trip for Avro schema-registry encoding
- **kafka:** Cover avro schema_file override path
- **kafka:** Integration round-trip for Protobuf schema-registry encoding
- **feedsource:** Kubernetes source signals Changes on CR add
- **feedsource:** K3s integration round-trip for the kubernetes source

### Build & CI

- Run the integration test suite on PRs and main
- **deps:** Add k8s.io/client-go and apimachinery for the CRD feed source

### Miscellaneous

- Gofmt struct-tag and test-literal alignment

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


