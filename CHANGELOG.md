# Changelog

All notable changes to this project are documented here.
This file is generated from [Conventional Commits](https://www.conventionalcommits.org)
by [git-cliff](https://git-cliff.org).

## [Unreleased]

### Features

- **model:** Canonical Change envelope with identity-key and content-hash helpers
- **retry:** Exponential backoff with full jitter
- **config:** Typed config with viper loader, env substitution, and startup validation
- **telemetry:** Zerolog + OTEL bootstrap with optional Prometheus exporter and Instruments
- **state:** Store interface and pgx-backed Postgres implementation
- **sink:** Publisher interface, Registry, retry+DLQ wrapper
- **sink/postgres:** Publisher with JSONB payload and within-poll idempotency
- **sink/kafka:** Publisher with franz-go, ItemID-keyed records, DLQ headers
- **sink:** Stub rabbitmq/sqs/sns publishers returning ErrNotImplemented
- **feed:** HTTP fetcher with cache validators and per-feed header overrides
- **feed:** Detector classifies items into new/updated against the state store
- **scheduler:** Per-feed ticker loop (serve) and bounded worker pool (run-once)
- **cmd:** Wire serve/run-once/validate-config end-to-end with OTEL instrumentation
- **config:** Coordination + SQS/SNS sink types and validation
- **coord:** Coordinator interface and noop implementation
- **coord/postgres:** Pg_try_advisory_lock-backed Coordinator
- **sink/sqs:** AWS SDK v2 implementation with LocalStack integration test
- **sink/sns:** AWS SDK v2 implementation with LocalStack integration test
- **cmd:** Wire coordinator, real SQS/SNS, PollSkipped instrument, ops docs
- **config:** Coordination redis driver types + validation
- **coord/redis:** Package skeleton with key derivation, options, and token helpers
- **coord/redis:** SET NX EX-backed Coordinator with renewal and CAS release
- **cmd:** Wire redis coordinator and document configuration
- **state/sqlite:** Pure-Go SQLite Store alongside Postgres
- **coord/redis:** Operator-level TLS knobs for rediss://
- **coord/postgres:** Operator-level TLS knobs (closes #4)
- **state/postgres:** Operator-level TLS knobs (closes #24)
- **sink/rabbitmq:** Real Publisher (closes #27)
- **sink/sqs:** FIFO queue support (closes #29)
- **sink/sns:** FIFO topic support (closes #30)
- **sink/stdout:** NDJSON debug sink (closes #10)
- **sink/http:** Generic webhook sink (closes #12)
- **feed:** Emit changes in published-ascending order
- **config:** Add feed_sources section
- **feedsource:** Source interface and canonical FeedSpec schema
- **feedsource:** Static source
- **feedsource:** Aggregator with precedence merge and last-known-good
- **feedsource:** File source with fsnotify watch and debounce
- **feedsource:** Generic poll-driven source helper
- **scheduler:** ServeDynamic reconciling feed loop with atomic reconcile
- **serve:** Dynamic feed reconcile from sources with SIGHUP reload
- **config:** Validate feed_sources entries
- **config:** Add feed sink config structs
- **config:** Validate feed sink and warn on multi-instance memory window
- **feed:** Store interface, synthetic id, memory window
- **feed:** Sqlite window backend
- **feed:** Postgres window backend (integration-tested)
- **feed:** Render changes to RSS 2.0 and Atom 1.0
- **feed:** Http handler with routing, render cache, conditional GET
- **feed:** Optional basic/bearer auth and Cache-Control headers
- **feed:** Publisher with listener, TLS, timeouts, DLQ skip, graceful close
- **feed:** OTEL request/304 metrics and zerolog server logging
- **cmd:** Wire the feed sink driver
- **config:** Add redis coordination mode + sentinel/cluster blocks
- **config:** Mode-aware redis coordination validation
- **coord/redis:** UniversalClient with per-mode client construction
- **cmd:** Wire redis coordination mode/sentinel/cluster options
- **config:** Register composite sink driver and config struct
- **config:** Validate composite children, dead_letter guard, cycle detection
- **sink:** Composite fan-out publisher with per-child outcomes and telemetry
- **cmd:** Wire composite sink via shared wrapSink helper and link pass
- Add multi-stage Docker config for dev and production

### Bug Fixes

- Prometheus registry, kafka traceparent, zerolog OTEL correlation, retry jitter, README
- **coord/postgres:** Release lock on canceled ctx; trim stale README bullet
- **config:** Tighten redis driver validation (review feedback)
- **coord/redis:** Drop dead context.Canceled branch and guard casDelete after Close (review feedback)
- **scheduler:** Reject runtime feeds without a valid interval
- **config:** Allow omitted feed.max_items to default to 50
- **coord/redis:** Repair sentinel integration test build (undefined net0)

### Refactor

- **coord:** Rename noop coordinator to memory
- **wire:** Extract pipeline factory shared by boot and reconcile
- **feedsource:** Idempotent File.Close and clarify aggregator concurrency
- **config:** Widen Validate to return ([]string, error) for warnings

### Documentation

- Add rss2msg design spec
- **rss2msg:** Add default-sink fallback, DLQ, and per-feed header overrides
- Add rss2msg implementation plan
- Add rss2msg v1.5 design (multi-instance + sqs/sns)
- Add rss2msg v1.5 implementation plan
- Add Redis coordinator design
- Add Redis coordinator implementation plan
- **readme:** Comprehensive guide covering config, sinks, state, coord
- **config.example:** Default to memory coordinator
- **config.example:** Default state to sqlite
- **config.example:** Default sink is now stdout
- **config.example:** Show explicit sinks: [...] on a feed
- Design for Obsidian end-user documentation restructure
- Implementation plan for Obsidian user-docs restructure
- Scaffold docs tree, link checker, and change-envelope reference
- Harden link checker against spaces in doc paths
- Reference pages (configuration, wire-formats, telemetry, cli) + forward-link stubs
- Keep verbatim 'dead-letter sink' link text in retry section
- Minor reference-page polish (runtime anchor, table whitespace)
- Core how-to pages (feeds, choose-a-sink, multi-instance, tls) + sink stubs
- Align drivers-table separator in choose-a-sink
- Per-driver sink pages (postgres, kafka, sqs, sns, rabbitmq, stdout, http)
- Getting-started tutorial + how-it-works/operations explanation pages
- Copy architecture diagram verbatim; match documented 304 wording in how-it-works
- Trim README to a landing page linking into docs/
- Gitignore churny Obsidian state, track vault transport snapshot
- Point wire-formats at rabbitmq/stdout/http pages; drop over-promising 'per driver'
- Replace README ASCII diagram with an Obsidian canvas
- Remove Design docs section from README
- Replace how-it-works ASCII diagram with canvas link
- Add developer (build/test, layout, contributing) and operator (deploy) guides
- Persist Obsidian canvas normalization (metadata block, formatting)
- Document dynamic feed sources
- Add dynamic feed list implementation plan
- Add comprehensive config.example.full.yaml; gitignore local/tooling state
- Add feed sink implementation plan
- Document the feed sink
- Document redis sentinel/cluster coordination modes
- **plan:** Add redis cluster/sentinel implementation plan
- **plan:** Composite sink implementation plan ([#45](https://github.com/IAmBod/rss2msg/issues/45))
- **sinks:** Document composite sink + config example
- **sinks:** Align choose-a-sink driver table
- Add AGENTS.md for AI coding agents
- **how-to:** Add Zapier and n8n connection guide
- Add CLAUDE.md and require worktrees under .worktrees/

### Testing

- **e2e:** End-to-end pipeline with Postgres + Kafka via testcontainers
- **feed:** Check http.Get error before using response (go vet)
- **coord/redis:** Sentinel lease lifecycle integration test
- **sink:** Nested-composite wiring and concurrent-publish coverage

### Build & CI

- Add gorilla/feeds dependency for the feed sink

### Miscellaneous

- Bootstrap rss2msg module and cobra skeleton
- **coord/redis:** Use time.Second constants in unit test (review feedback)
- Replace Makefile with Taskfile.yaml
- Gitignore .task/ and drop accidentally committed fingerprint
- Lowercase Taskfile.yaml → taskfile.yaml
- Gitignore sqlite runtime artifacts and drop accidental commit
- Untrack unrelated dynamic-feed-list plan (accidental add)
- Untrack dynamic-feed-list plan (re-added by a broad git add)
- Untrack .vault-meta (per-machine Obsidian tooling state, not source)
- Gitignore local working config.yaml


