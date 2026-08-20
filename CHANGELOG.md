# Changelog

All notable changes to this project are documented here.
This file is generated from [Conventional Commits](https://www.conventionalcommits.org)
by [git-cliff](https://git-cliff.org).

## [0.4.0] - 2026-08-20

### Features

- **feed-sink:** Per-surface multi-credential auth model ([#131](https://github.com/IAmBod/rss2msg/issues/131))
- **config:** Per-surface feed-sink auth schema + validation ([#131](https://github.com/IAmBod/rss2msg/issues/131))
- **feed-sink:** Wire per-surface auth from config ([#131](https://github.com/IAmBod/rss2msg/issues/131))
- **config:** Add feed sink trusted_proxies field and validation ([#171](https://github.com/IAmBod/rss2msg/issues/171))
- **feed:** Trusted-proxy allowlist parser and membership test ([#171](https://github.com/IAmBod/rss2msg/issues/171))
- **feed:** Derive self-URL from trusted forwarding headers ([#171](https://github.com/IAmBod/rss2msg/issues/171))
- **feed:** Recover real client IP from trusted X-Forwarded-For ([#171](https://github.com/IAmBod/rss2msg/issues/171))
- **feed:** Assemble self-URL per request over cached self-less body ([#171](https://github.com/IAmBod/rss2msg/issues/171))
- **feed:** Log real client IP on auth failure ([#171](https://github.com/IAmBod/rss2msg/issues/171))
- **feed:** Plumb trusted_proxies into publisher, drop init-time self URLs ([#171](https://github.com/IAmBod/rss2msg/issues/171))
- **cmd:** Pass feed sink trusted_proxies into the publisher ([#171](https://github.com/IAmBod/rss2msg/issues/171))
- **sink:** Wire amqp10 config, validation, and broker-agnostic publisher
- **sink:** Wire rabbitmq_stream config and validation
- **sink:** Add rabbitmq_stream sink (native stream protocol)
- **retry:** Optional Retryable predicate to stop on terminal errors
- **feed:** Typed FetchError and IsRetryable classifier
- **config:** Http.retry global + per-feed fetch retry config
- **pipeline:** In-tick fetch retry on transient errors
- **assign:** Rendezvous-hash feed ownership function
- **coord:** Membership interface, member-id helper, memory single-member backend
- **config:** Coordination.assignment.* keys, defaults, validation
- **telemetry:** Membership size, assigned feeds, rebalance event meters
- **scheduler:** Ownership-filtering feed provider with heartbeat loop
- **coord/redis:** TTL-keyed membership backend
- **coord/postgres:** Coordination_members heartbeat table
- **coord/dynamodb:** Member-item heartbeat + scan-based live set
- **coord/cosmosdb:** Member-doc heartbeat + query-based live set
- **serve:** Wire membership/assignment provider into the daemon
- **heartbeat:** Opt-in liveness heartbeat log ([#189](https://github.com/IAmBod/rss2msg/issues/189))
- **state:** Add PruneItemsBefore to sqlite store
- **state:** Add PruneItemsBefore to postgres store
- **state:** Add no-op PruneItemsBefore to dynamodb and cosmosdb stores
- **state:** Require PruneItemsBefore on the Store interface
- **config:** Unify state.item_ttl and add SQL cleanup_interval
- **state:** Add statecleanup periodic sweep loop
- **serve:** Run TTL state cleanup sweep for SQL backends
- **state:** Add PruneFeedMetaBefore to sqlite store
- **state:** Add PruneFeedMetaBefore to postgres store
- **state:** Prune feed_meta via native TTL on dynamodb/cosmosdb meta rows
- **state:** Sweep feed_meta alongside seen_items in statecleanup
- **httpauth:** Shared bearer/basic/api-key auth core ([#185](https://github.com/IAmBod/rss2msg/issues/185))
- **config:** Admin API config types, defaults, httpauth converter ([#185](https://github.com/IAmBod/rss2msg/issues/185))
- **config:** Validate admin API config (auth/tls/mtls/cors) ([#185](https://github.com/IAmBod/rss2msg/issues/185))
- **scheduler:** Optional PollNow channel for off-cycle feed polls ([#185](https://github.com/IAmBod/rss2msg/issues/185))
- **admin:** Server skeleton, /v1/status, auth + CORS middleware ([#185](https://github.com/IAmBod/rss2msg/issues/185))
- **admin:** /v1/feeds and /v1/feeds/{id} with ownership + meta ([#185](https://github.com/IAmBod/rss2msg/issues/185))
- **admin:** /v1/members with ownership map; OwnerProvider accessors ([#185](https://github.com/IAmBod/rss2msg/issues/185))
- **admin:** /v1/health dependency-ping endpoint ([#185](https://github.com/IAmBod/rss2msg/issues/185))
- **admin:** POST /v1/feeds/reconcile ([#185](https://github.com/IAmBod/rss2msg/issues/185))
- **admin:** POST /v1/feeds/{id}/poll ([#185](https://github.com/IAmBod/rss2msg/issues/185))
- **admin:** POST /v1/state/prune with duration defaults ([#185](https://github.com/IAmBod/rss2msg/issues/185))
- **admin:** TLS and mTLS (client-cert) support for the listener ([#185](https://github.com/IAmBod/rss2msg/issues/185))
- **admin:** Wire admin API into serve (deps, pollNow, lifecycle) ([#185](https://github.com/IAmBod/rss2msg/issues/185))

### Bug Fixes

- **release:** Drop [skip ci] from changelog commit so tag triggers release
- **config:** Reject feed-sink api_key_header with no api_keys ([#131](https://github.com/IAmBod/rss2msg/issues/131))
- **feed:** Drop forwarded prefix when host falls back to request Host ([#171](https://github.com/IAmBod/rss2msg/issues/171))
- **sink:** Block unconditionally on stream publish confirmation after send
- **feed:** Classify inner transport error for context.Canceled
- **coord:** Hash feed URL for DynamoDB & Cosmos DB lock keys
- **telemetry:** Order test imports for goimports
- **coord/dynamodb:** Capture single now() in membership heartbeat
- **coord:** Reap expired member entries on heartbeat (dynamodb, cosmosdb)
- **assignment:** Prime membership before first reconcile to avoid startup churn
- **feedsource/kubernetes:** Correct CRD manifest path in k3s test
- **sink/dapr:** Use NewClientWithAddressContext for client setup
- **ci:** Keep the CLA job inert until CLA_SIGNATURES_TOKEN is set
- **deps:** Bump moby/go-archive to v0.3.3 for the tar path-traversal fix

### Performance

- **release:** Warm cross-compile build cache to speed up releases

### Refactor

- **feedsource:** Split pull sources into per-backend subpackages
- **cmd:** Extract serve/run-once/validate-config into own files
- **feed:** Normalize bare-IP CIDR and tidy proxy tests ([#171](https://github.com/IAmBod/rss2msg/issues/171))
- **feed:** Make self-link injection per-request, add RSS self-link ([#171](https://github.com/IAmBod/rss2msg/issues/171))
- **feed:** Align auth-log field, guard rss namespace dup, assert per-host etag ([#171](https://github.com/IAmBod/rss2msg/issues/171))
- **sink:** Rename rabbitmq driver to amqp091
- **sink:** Rename stale rabbitmq reference in TLS-collect comment
- **state:** Move sqlite test-only updated_at seam to export_test.go
- **feed:** Delegate auth to internal/httpauth ([#185](https://github.com/IAmBod/rss2msg/issues/185))

### Documentation

- **specs:** Feed sink auth hardening design ([#131](https://github.com/IAmBod/rss2msg/issues/131))
- **plans:** Feed sink auth PR-A implementation plan ([#131](https://github.com/IAmBod/rss2msg/issues/131))
- **feed-sink:** Document per-surface multi-credential auth ([#131](https://github.com/IAmBod/rss2msg/issues/131))
- **plans:** Feed sink auth PR-B (mTLS) implementation plan ([#131](https://github.com/IAmBod/rss2msg/issues/131))
- **plans:** Feed sink reverse-proxy support design ([#171](https://github.com/IAmBod/rss2msg/issues/171))
- **plans:** Feed sink reverse-proxy implementation plan ([#171](https://github.com/IAmBod/rss2msg/issues/171))
- **feed:** Document trusted_proxies and reverse-proxy self-URLs ([#171](https://github.com/IAmBod/rss2msg/issues/171))
- **plans:** Correct spec — client IP is log-only, Forwarded for= not parsed ([#171](https://github.com/IAmBod/rss2msg/issues/171))
- **plans:** AMQP 1.0 + RabbitMQ Streams sinks design, with rabbitmq→amqp091 rename
- **plans:** Implementation plans for amqp091 rename + amqp10 + rabbitmq_stream sinks
- Add Discord & Slack via Redpanda Connect integration guide
- Spec for per-feed backoff on fetch failures
- Implementation plan for per-feed fetch backoff
- Document http.retry feed-fetch backoff (global + per-feed)
- **spec:** Assignment/partition model design ([#183](https://github.com/IAmBod/rss2msg/issues/183))
- **spec:** Deregister-on-shutdown for assignment membership ([#183](https://github.com/IAmBod/rss2msg/issues/183))
- **spec:** Rebalance walkthroughs + N>M standby behavior ([#183](https://github.com/IAmBod/rss2msg/issues/183))
- **spec:** Feed-set change walkthrough; orthogonality with membership ([#183](https://github.com/IAmBod/rss2msg/issues/183))
- **spec:** Parameter relationships — membership timing vs feed interval ([#183](https://github.com/IAmBod/rss2msg/issues/183))
- **spec:** Poll duration vs lock TTL interaction ([#183](https://github.com/IAmBod/rss2msg/issues/183))
- **plan:** TDD implementation plan for assignment model ([#183](https://github.com/IAmBod/rss2msg/issues/183))
- Explain and reference the coordination assignment model
- Fix fact count and redundant member_ttl constraint wording
- **heartbeat:** Design spec for opt-in liveness heartbeat log ([#189](https://github.com/IAmBod/rss2msg/issues/189))
- **spec:** State store cleanup (TTL pruning of seen items) design
- **plan:** State store cleanup implementation plan
- **state:** Document unified item_ttl and SQL cleanup_interval
- **state:** Align cosmosdb/dynamodb summaries with native-TTL bodies
- **plan:** Feed_meta cleanup addendum (item_ttl, updated_at anchor)
- **state:** Document feed_meta cleanup via item_ttl
- **state:** Update spec Testing section for feed_meta prune coverage
- **state:** Correct per-backend meta-row TTL docs after feed_meta cleanup
- **admin:** Design spec for opt-in Admin API ([#185](https://github.com/IAmBod/rss2msg/issues/185))
- **admin:** Add dashboard forward-compat hooks to spec ([#185](https://github.com/IAmBod/rss2msg/issues/185))
- **admin:** Auth.enabled (default on) + mTLS for Admin API ([#185](https://github.com/IAmBod/rss2msg/issues/185))
- **admin:** Implementation plan + spec fixes for Admin API ([#185](https://github.com/IAmBod/rss2msg/issues/185))
- **admin:** Config example + admin API reference and how-to ([#185](https://github.com/IAmBod/rss2msg/issues/185))
- **admin:** Correct auth-runtime-behavior description ([#185](https://github.com/IAmBod/rss2msg/issues/185))
- **license:** Add Contributor License Agreement and signing workflow

### Testing

- **feed-sink:** Cover authFailReason; doc timing tradeoff ([#131](https://github.com/IAmBod/rss2msg/issues/131))
- **feed-sink:** Assert auth metric attributes and api-key handler path ([#131](https://github.com/IAmBod/rss2msg/issues/131))
- **feed:** Pin client-IP behavior for non-IP XFF tokens and peerIP ([#171](https://github.com/IAmBod/rss2msg/issues/171))
- **sink:** Amqp10 integration test; docs and config examples
- **sink:** Rabbitmq_stream integration test; docs, examples, CI shard
- **scheduler:** Require baseline signal; document single heartbeat driver
- **coord/redis:** Assert membership interface; check heartbeat errors
- **coord/dynamodb:** Target hashed lock key in lease integration tests (#184 regression)
- **coord/cosmosdb:** Partition-faithful fake query; log dropped member docs
- **config:** Drop duplicate dynamodb TTL test, add negative cleanup_interval test
- **state:** Assert item and feed_meta pruned with the same cutoff
- **feed:** Characterize auth behavior before httpauth extraction ([#185](https://github.com/IAmBod/rss2msg/issues/185))

### Build & CI

- **integration:** Prune containers and images between test packages
- **integration:** Shard integration tests across runners by heavy image
- **deps:** Bump golang.org/x/crypto
- **deps:** Configure Dependabot version updates
- **deps:** Bump azure/setup-helm from 4 to 5
- **deps:** Bump docker/setup-buildx-action from 3 to 4
- **deps:** Bump actions/checkout from 4 to 7
- **deps:** Bump actions/cache from 4 to 6
- **deps:** Bump actions/setup-go from 5 to 7

### Dependencies

- **deps:** Bump the go-modules group across 1 directory with 57 updates
- **deps:** Hold gofeed at v1.3.0 pending a parse-path regression
- **deps:** Bump google.golang.org/grpc from 1.81.1 to 1.82.1
- **deps:** Bump golang from 1.25-bookworm to 1.26-bookworm

### Miscellaneous

- **cmd:** Gofmt feed Options literal alignment ([#171](https://github.com/IAmBod/rss2msg/issues/171))
- Gofmt struct alignment and trailing newline
- **state:** Gofmt mock alignment, clarify PruneItemsBefore test comment
- **admin:** Drop dead import-anchor sentinels in handlers.go ([#185](https://github.com/IAmBod/rss2msg/issues/185))
- Change contact address to info@iambod.dev
- Use info@iambod.dev in the CLA docs

## [0.3.0] - 2026-06-15

### Features

- **release:** Add /release command and cut-release skill

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


