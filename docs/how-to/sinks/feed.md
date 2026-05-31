---
title: Feed sink
type: how-to
tags: [rss2msg/docs, sinks, feed, rss, atom]
summary: Re-publish detected changes as an RSS 2.0 and Atom 1.0 feed over HTTP so feed readers can subscribe.
updated: 2026-05-31
---

# Feed sink

Re-publishes detected changes as a feed served over HTTP, so any feed reader
can subscribe to rss2msg's output. The sink runs its own HTTP server and
exposes two endpoints: `GET /rss` (RSS 2.0) and `GET /atom` (Atom 1.0). It
keeps a rolling window of the most recent changes and renders them on request.

```yaml
sinks:
  - name: out-feed
    driver: feed
    feed:
      listen: ":8088"                 # required — HTTP listen address
      public_url: "https://feeds.example.com"  # external base URL of this feed, for Atom rel=self (default: link)
      title: "rss2msg — changes"      # feed/channel title (default: sink name)
      link: "https://example.com/"    # the website this feed is about (RSS channel link / Atom rel=alternate)
      description: "Aggregated feed changes from rss2msg"
      max_items: 50                   # rolling window size (default 50)
      rss_path: /rss                  # default /rss
      atom_path: /atom                # default /atom
      render_cache_ttl: 10s           # optional; in-memory server-side render cache, pure TTL (default off)
      cache_control_ttl: 5m           # optional; Cache-Control: public, max-age=<this> (default off -> no-cache)
      timeouts:                       # optional; safe non-zero defaults applied when unset
        read_header: 5s
        read: 10s
        write: 15s
        idle: 60s
        shutdown: 5s
      tls:                            # optional; serve HTTPS directly. Omit when TLS is terminated upstream.
        cert_file: /etc/rss2msg/feed.crt
        key_file: /etc/rss2msg/feed.key
      auth:                           # optional; when set, endpoints require auth and responses become Cache-Control: private
        basic: { username: feeds, password: ${FEED_PASSWORD} }
        # or, instead of basic:  bearer_token: ${FEED_TOKEN}
      store:
        driver: memory                # default; memory | sqlite | postgres
        sqlite:
          path: /var/lib/rss2msg/feed.db   # required when driver=sqlite
        postgres:
          dsn: ${POSTGRES_DSN}             # required when driver=postgres
          table: feed_output               # default; identifier-validated
          tls: { ca_file: ..., cert_file: ..., key_file: ..., server_name: ..., insecure_skip_verify: false }
```

| field               | required | default       | notes |
| ------------------- | -------- | ------------- | ----- |
| `listen`            | yes      | —             | HTTP listen address (`host:port` or `:port`). |
| `public_url`        | no       | value of `link` | Externally-reachable base URL of this feed; used for Atom `rel=self`. Must be an absolute URL when set. |
| `title`             | no       | sink `name`   | Feed / RSS channel title. |
| `link`              | no       | (none)        | The website this feed is about (RSS channel link / Atom `rel=alternate`). Must be an absolute URL when set. |
| `description`       | no       | (none)        | Feed / channel description. |
| `max_items`         | no       | `50`          | Rolling window size. Must be `>= 1`. |
| `rss_path`          | no       | `/rss`        | Must start with `/` and differ from `atom_path`. |
| `atom_path`         | no       | `/atom`       | Must start with `/` and differ from `rss_path`. |
| `render_cache_ttl`  | no       | off           | In-memory server-side render cache (pure TTL); see [HTTP caching](#http-caching). |
| `cache_control_ttl` | no       | off           | Client-facing `max-age`; see [HTTP caching](#http-caching). |
| `timeouts`          | no       | (see below)   | HTTP server timeouts. |
| `tls`               | no       | (none)        | Serve HTTPS directly; `cert_file` and `key_file` must both be set or both empty. |
| `auth`              | no       | (none)        | Exactly one of `basic` or `bearer_token`; see [Auth](#auth). |
| `store`             | no       | `memory`      | Backing window store; see [Store backends](#store-backends). |

`timeouts` are applied with safe non-zero defaults when unset: `read_header: 5s`,
`read: 10s`, `write: 15s`, `idle: 60s`, `shutdown: 5s`.

A feed sink cannot be used as a dead-letter target. Error envelopes (changes
delivered via DLQ routing) are never surfaced in the public feed.

## Endpoints

| request                  | response |
| ------------------------ | -------- |
| `GET <rss_path>`         | `200`, `Content-Type: application/rss+xml` (RSS 2.0). |
| `GET <atom_path>`        | `200`, `Content-Type: application/atom+xml` (Atom 1.0). |
| `HEAD` on either path    | `200` with headers, no body. |
| any other method         | `405`, `Allow: GET, HEAD`. |
| any other path           | `404`. |

The feed-item layout (how a `Change` maps onto a feed entry, and the synthetic
entry id) is documented in [Sink Wire Formats](../../reference/wire-formats.md).

## Store backends

The window store holds the most recent changes (keyed on `(feed_url, item_id)`,
so re-detecting the same item updates its existing entry). On each request the
sink reads up to `max_items` most-recent changes and renders them.

| driver     | use it for |
| ---------- | ---------- |
| `memory`   | Default. Zero-dependency, single-instance. The window is lost on restart but refills as new changes are detected. |
| `sqlite`   | Durable single-instance. Survives restarts. Requires `store.sqlite.path`. |
| `postgres` | Shared window. Required for multi-instance correctness — every instance serves the identical feed. Requires `store.postgres.dsn`. |

For `postgres`, `store.postgres.table` defaults to `feed_output` and is
identifier-validated (never interpolated raw). `store.postgres.tls` configures
TLS to the database; `cert_file` and `key_file` must both be set or both empty,
and TLS cannot be combined with a DSN that has `sslmode=disable`.

## Multi-instance

Each instance binds its own HTTP server. With a shared `postgres` window, every
instance renders the identical feed, so scaling out means putting a load
balancer in front of the instances. With `memory` or `sqlite` behind a load
balancer, each instance has its own window and readers receive partial feeds —
config validation emits a warning when coordination looks multi-instance but
the feed window is not `postgres`.

See [Run Multiple Instances](../run-multiple-instances.md) for coordination
setup.

## HTTP caching

Two independent knobs control caching:

- `render_cache_ttl` — server-side render cache. The rendered RSS/Atom document
  is cached in memory for this duration (pure TTL) so repeat requests within the
  window skip re-querying the store and re-rendering. Off by default.
- `cache_control_ttl` — client-facing cache. Sets `Cache-Control: <scope>,
  max-age=<seconds>`. When unset, responses are `Cache-Control: <scope>,
  no-cache`. The `<scope>` is `public` normally, and `private` when `auth` is
  set.

Every response carries an `ETag` (sha256 of the rendered body) and a
`Last-Modified` header. Conditional requests are honored: a matching
`If-None-Match` or a satisfied `If-Modified-Since` returns `304 Not Modified`.

## Auth

`auth` is optional. When set, both endpoints require authentication and
responses switch to `Cache-Control: private`. Configure **exactly one** of:

- `basic` — HTTP Basic auth; both `username` and `password` are required. An
  unauthenticated request gets `401` with `WWW-Authenticate: Basic realm="rss2msg"`.
- `bearer_token` — the request must carry `Authorization: Bearer <token>`.

Credential comparisons are constant-time.

## TLS vs reverse proxy

To serve HTTPS directly from rss2msg, set `tls.cert_file` and `tls.key_file`.
Omit the `tls` block to serve plain HTTP and terminate TLS upstream (e.g. behind
a reverse proxy or load balancer).

When running behind a proxy, set `public_url` to the externally-reachable base
URL so the Atom `rel=self` link is correct — rss2msg does not read
`X-Forwarded-*` headers, so it cannot infer the external URL on its own. If
`public_url` is unset it falls back to `link`.

See [Secure Connections (TLS)](../secure-connections-tls.md) for certificate
guidance.

## Related

- [Choose a Sink](../choose-a-sink.md) — all drivers and the decision table.
- [Sink Wire Formats](../../reference/wire-formats.md) — the RSS/Atom item layout.
- [Change Envelope](../../reference/change-envelope.md) — the source record.
</content>
</invoke>
