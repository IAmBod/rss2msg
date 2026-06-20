---
title: Feed sink
type: how-to
tags: [rss2msg/docs, sinks, feed, rss, atom, mcp]
summary: Re-publish detected changes as an RSS 2.0, Atom 1.0, or MCP surface over HTTP (with optional HTTP/3) so feed readers and AI agents can subscribe.
updated: 2026-06-17
---

# Feed sink

Re-publishes detected changes over HTTP from a rolling window of the most
recent changes, rendered on request. It runs its own HTTP server and can serve
the same window through three independent **surfaces**, each a `{ enabled, path }`
block: `rss` (RSS 2.0, default `/rss`), `atom` (Atom 1.0, default `/atom`), and
`mcp` (a Model Context Protocol server for AI agents, default `/mcp`). RSS and
Atom are enabled by default; MCP is opt-in. All three share the listener, TLS,
and auth, and at least one must be enabled.

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
      rss:                            # RSS 2.0 output surface (default enabled at /rss)
        enabled: true
        path: /rss
      atom:                           # Atom 1.0 output surface (default enabled at /atom)
        enabled: true
        path: /atom
      mcp:                            # MCP server surface (opt-in; default disabled)
        enabled: false
        path: /mcp
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
      http3: false                    # optional; also serve HTTP/3 (QUIC) on the same port. Requires tls.
      trusted_proxies: []             # optional; CIDRs and/or presets (private, all). Empty => forwarding headers ignored.
      auth:                           # optional default for all surfaces; see Auth section
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
| `rss`               | no       | enabled `/rss`  | RSS 2.0 surface: `{ enabled, path }`. `path` must start with `/` and differ from other enabled surfaces. |
| `atom`              | no       | enabled `/atom` | Atom 1.0 surface: `{ enabled, path }`. `path` must start with `/` and differ from other enabled surfaces. |
| `mcp`               | no       | disabled `/mcp` | MCP surface: `{ enabled, path }`. Opt-in. See [MCP surface](#mcp-surface). |
| `render_cache_ttl`  | no       | off           | In-memory server-side render cache (pure TTL); see [HTTP caching](#http-caching). |
| `cache_control_ttl` | no       | off           | Client-facing `max-age`; see [HTTP caching](#http-caching). |
| `timeouts`          | no       | (see below)   | HTTP server timeouts. |
| `tls`               | no       | (none)        | Serve HTTPS directly; `cert_file` and `key_file` must both be set or both empty. |
| `http3`             | no       | `false`       | Also serve HTTP/3 (QUIC) on the same UDP port and advertise it via `Alt-Svc`; see [HTTP/3](#http3). Requires `tls`. |
| `trusted_proxies`   | no       | `[]` (none)   | Upstream proxies whose `X-Forwarded-*` / `Forwarded` headers are honored, as CIDRs and/or presets (`private` = RFC1918 + loopback + ULA; `all` = any). Empty disables all header parsing. See [Behind a reverse proxy](#tls-vs-reverse-proxy). |
| `auth`              | no       | (none)        | Default credential policy for all surfaces; per-surface `auth` blocks fully replace this default. See [Auth](#auth). |
| `store`             | no       | `memory`      | Backing window store; see [Store backends](#store-backends). |

`timeouts` are applied with safe non-zero defaults when unset: `read_header: 5s`,
`read: 10s`, `write: 15s`, `idle: 60s`, `shutdown: 5s`.

A feed sink cannot be used as a dead-letter target. Error envelopes (changes
delivered via DLQ routing) are never surfaced in the public feed.

## Endpoints

| request                  | response |
| ------------------------ | -------- |
| `GET <rss.path>`         | `200`, `Content-Type: application/rss+xml` (RSS 2.0). |
| `GET <atom.path>`        | `200`, `Content-Type: application/atom+xml` (Atom 1.0). |
| `HEAD` on either path    | `200` with headers, no body. |
| `<mcp.path>`             | MCP streamable-HTTP endpoint when `mcp.enabled`; see [MCP surface](#mcp-surface). |
| any other method (rss/atom) | `405`, `Allow: GET, HEAD`. |
| any other path           | `404`. |

The feed-item layout (how a `Change` maps onto a feed entry, and the synthetic
entry id) is documented in [Sink Wire Formats](../../reference/wire-formats.md).

## MCP surface

When `mcp.enabled` is set, the sink also serves a [Model Context
Protocol](https://modelcontextprotocol.io) server at `mcp.path` (default `/mcp`)
over the streamable-HTTP transport, on the same listener and behind the same TLS
and auth as the RSS/Atom surfaces. This lets an AI agent read the same rolling
window the feed renders. It is **read-only**: there are no tools that mutate
state.

Connect an MCP client to `http(s)://<host><mcp.path>`. The server exposes four
tools, all backed by the rolling window (so results are bounded by `max_items`):

| tool | arguments | returns |
| ---- | --------- | ------- |
| `list_feeds` | — | the feeds in the window, each with an item count. |
| `list_recent_items` | `limit` (optional, default/cap = `max_items`), `feed_url` (optional) | recent items newest-first (summaries, no body). |
| `get_item` | `feed_url`, `item_id` | a single item including full content. |
| `search_items` | `query`, `since` (optional, RFC3339) | items whose title/summary/content contain `query` (case-insensitive), at or after `since`. |

Each item carries its native `item_id` (the key `get_item` expects) plus the
synthetic `guid` URN that the RSS/Atom output emits, so MCP results
cross-reference the syndication feed. The window only contains items routed to
*this* feed sink — it is not a global archive of every feed.

## Store backends

> [!warning]
> **The default `memory` store does not survive a restart.** Its window lives
> only in the process, so after a restart the served `/rss` and `/atom` feeds
> come back empty and refill only as feeds are re-polled *and change again* —
> items the upstream has since rotated out never return. For a feed that must
> persist across restarts use `store.driver: sqlite` (durable local file) or
> `postgres` (durable and shared across instances).

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

`auth` is optional. A feed sink with no `auth:` block at all is public.

### Default and per-surface auth

The top-level `auth:` block under the feed sink config is the **default for all
surfaces** (`rss`, `atom`, `mcp`). Each surface (`rss`, `atom`, `mcp`) may carry
its own `auth:` block that **fully replaces** the default for that surface — there is
no field-level merging, the surface block wins entirely. A surface with
`auth: {disabled: true}` is public regardless of the default.

When a surface is authenticated, responses switch to `Cache-Control: private` and an
unauthenticated (or incorrectly credentialed) request receives **HTTP 401** with a
`WWW-Authenticate` header (`Basic` when basic users are configured for that surface,
otherwise `Bearer`).

### Credential methods

All credential fields are optional and any combination is valid. **Any one valid
credential authenticates** (OR semantics — the request succeeds if it matches any
entry in any configured list).

- `basic_users` — a list of HTTP Basic users. Each entry has an optional `name`,
  a required `username`, and a required `password`.
- `bearer_tokens` — a list of bearer-token entries. Each entry has an optional `name`
  and a required `token` matched against `Authorization: Bearer <token>`.
- `api_keys` — a list of API key entries. Each entry has an optional `name` and a
  required `key`.
- `api_key_header` — the request header API keys are read from. Defaults to
  `X-API-Key` when omitted. Only valid when `api_keys` is set.

The optional `name` on each credential is for observability: it appears as the
`credential` attribute on the `feed_sink.auth_success` metric. Authentication
failures increment `feed_sink.auth_failure` with a `reason` attribute
(`no_credentials` or `bad_token`). Both metrics also carry a `surface` attribute
(`rss`, `atom`, or `mcp`).

All credential comparisons are constant-time.

### Example

```yaml
sinks:
  - name: myfeed
    driver: feed
    feed:
      listen: "0.0.0.0:8443"
      auth:                              # default for all surfaces
        basic_users:
          - {name: alice, username: alice, password: s3cret}
        bearer_tokens:
          - {name: ci-bot, token: tok_a}
        api_keys:
          - {name: partner-x, key: key_1}
        api_key_header: X-API-Key        # default when omitted
      rss:  {enabled: true, auth: {disabled: true}}    # public
      atom: {enabled: true}                            # inherits the default
      mcp:  {enabled: true, auth: {bearer_tokens: [{name: mcp, token: t_mcp}]}}
```

### Validation

Config validation rejects:

- A per-surface `auth` block that defines no credential method and is not `disabled: true`.
- `disabled: true` combined with any credentials on the same block.
- A `basic_users` entry missing `username` or `password`.
- A `bearer_tokens` or `api_keys` entry missing its secret field.
- Duplicate `name` values within one credential type on the same block.
- An `api_key_header` that contains spaces, tabs, or colons.
- An `api_key_header` set without any `api_keys`.

> **Note:** mTLS client-certificate auth is planned as PR-B of issue #131 and is not yet available.

## TLS vs reverse proxy

To serve HTTPS directly from rss2msg, set `tls.cert_file` and `tls.key_file`.
Omit the `tls` block to serve plain HTTP and terminate TLS upstream (e.g. behind
a reverse proxy or load balancer).

By default the feed sink ignores all forwarding headers, so behind a proxy you
must set `public_url` to the externally-reachable base URL for the Atom/RSS
`rel=self` links to be correct (it falls back to `link` when unset).

Alternatively, list your proxies in `trusted_proxies` (CIDRs, or the presets
`private` / `all`). When a request's direct peer is in that set, rss2msg derives
the self-URL from `X-Forwarded-Proto`, `X-Forwarded-Host`, and `X-Forwarded-Prefix`
(or the RFC 7239 `Forwarded` header), and recovers the real client IP from
`X-Forwarded-For` for auth-failure logs. `public_url`, when set, always wins over
forwarding headers. Headers from an untrusted peer are never honored, so a client
hitting the listener directly cannot spoof its self-URL or client IP. The proxy
is expected to strip any `X-Forwarded-Prefix` before forwarding; rss2msg only
prepends it to self-links and does not rewrite internal routes. `trusted_proxies:
[all]` honors headers from any source — only safe when the listener is not
publicly reachable.

See [Secure Connections (TLS)](../secure-connections-tls.md) for certificate
guidance.

## HTTP/3

Set `http3: true` to additionally serve the feed over **HTTP/3** (QUIC). HTTP/3
runs over UDP and is TLS-only, so `tls.cert_file` and `tls.key_file` are
required — config validation rejects `http3` without them.

When enabled, the sink binds a UDP socket on the **same** `host:port` as the TCP
listener and serves HTTP/3 there, while the TCP listener keeps serving
HTTP/1.1 and HTTP/2. The TCP responses carry an `Alt-Svc: h3=":<port>"` header so
clients that understand HTTP/3 can upgrade on their next request. Clients that
don't simply keep using HTTP/2 — enabling `http3` never breaks existing readers.

Make sure UDP on the feed port is reachable end-to-end (firewall / load balancer
/ security-group rules) — HTTP/3 fails closed to HTTP/2 if UDP is blocked.

## Related

- [Choose a Sink](../choose-a-sink.md) — all drivers and the decision table.
- [Sink Wire Formats](../../reference/wire-formats.md) — the RSS/Atom item layout.
- [Change Envelope](../../reference/change-envelope.md) — the source record.
