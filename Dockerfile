# syntax=docker/dockerfile:1
#
# Multi-stage build for rss2msg.
#
#   development — full Go toolchain + `air` for hot reload (used by docker-compose).
#   production  — static binary on a distroless base; small and runs as nonroot.
#
# Build either stage explicitly with --target:
#   docker build --target development -t rss2msg:dev .
#   docker build --target production  -t rss2msg:latest .
#
# rss2msg uses a pure-Go SQLite driver (modernc.org/sqlite), so the binary builds
# with CGO disabled and needs no libc at runtime — hence the `static` distroless base.

############################
# base — shared module cache
############################
FROM golang:1.25-bookworm AS base
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
# 8080: feed-sink HTTP (when a feed sink is configured); 9090: Prometheus /metrics.
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
# production — minimal runtime
############################
FROM gcr.io/distroless/static-debian12:nonroot AS production
COPY --from=build /out/rss2msg /usr/local/bin/rss2msg
# Config is resolved from --config, then ./config.yaml, then /etc/rss2msg/config.yaml.
# Mount your config at /etc/rss2msg/config.yaml (see docs/how-to/run-with-docker.md).
EXPOSE 8080 9090
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/rss2msg"]
CMD ["serve"]
