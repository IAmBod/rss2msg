# syntax=docker/dockerfile:1
#
# Multi-stage build for rss2msg.
#
#   development — full Go toolchain + `air` for hot reload (used by docker-compose).
#   build       — compiles the static binary from source. Handy on its own
#                 (`docker build --target build --output=. .`); the production image
#                 below does NOT use it (GoReleaser supplies that binary).
#   production  — distroless runtime that packages a prebuilt binary. This is the
#                 final stage, so `docker build` targets it by default — which is how
#                 GoReleaser (dockers_v2) builds the published image.
#
# The production stage does NOT compile from source: GoReleaser cross-compiles the
# binary (see builds: in .goreleaser.yaml) and stages it in the build context under
# a per-platform subdirectory (linux/amd64/rss2msg, linux/arm64/rss2msg, …), which
# the `COPY $TARGETPLATFORM/rss2msg` below selects. A plain `docker build .` has no
# such binary in its context; build a local production image with `task release-snapshot`
# (GoReleaser) instead. The hot-reload `development` image still builds from source.
#
# rss2msg uses a pure-Go SQLite driver (modernc.org/sqlite), so the binary builds
# with CGO disabled and needs no libc at runtime — hence the `static` distroless base.

############################
# base — shared module cache
############################
FROM golang:1.26-bookworm AS base
WORKDIR /src
# Download modules first so this layer is cached until go.mod/go.sum change.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

############################
# development — hot reload via air
############################
FROM base AS development
# air watches the source and rebuilds on change. Pin-free: this is a dev-only tool.
RUN --mount=type=cache,target=/go/pkg/mod go install github.com/air-verse/air@latest
COPY . .
# 8080: health probes (/healthz, /readyz, /startupz on serve) + feed-sink HTTP when
# a feed sink is configured; 9090: Prometheus /metrics.
EXPOSE 8080 9090
ENTRYPOINT ["air", "-c", ".air.toml"]

############################
# build — compile the static binary
############################
FROM base AS build
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
    -o /out/rss2msg ./cmd/rss2msg

############################
# production — minimal runtime (final stage = default build target)
############################
FROM gcr.io/distroless/static-debian12:nonroot AS production
# GoReleaser (dockers_v2) stages the cross-compiled binary under per-platform
# subdirectories; $TARGETPLATFORM (set by buildx, e.g. linux/amd64) selects ours.
ARG TARGETPLATFORM
COPY $TARGETPLATFORM/rss2msg /usr/local/bin/rss2msg
# Config is resolved from --config, then ./config.yaml, then /etc/rss2msg/config.yaml.
# Mount your config at /etc/rss2msg/config.yaml (see docs/how-to/run-with-docker.md).
# 8080: health probes (/healthz, /readyz, /startupz on serve) + feed-sink HTTP when
# a feed sink is configured; 9090: Prometheus /metrics.
EXPOSE 8080 9090
USER nonroot:nonroot
# The distroless base ships no shell, curl, or wget, so the classic
# `HEALTHCHECK CMD curl …` cannot run. Instead the binary probes its own health
# endpoint and exits 0/1 — see `rss2msg healthcheck`. It reads the same config as
# `serve`, so it targets whatever address `health.listen` binds (default :8080).
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
    CMD ["/usr/local/bin/rss2msg", "healthcheck"]
ENTRYPOINT ["/usr/local/bin/rss2msg"]
CMD ["serve"]
