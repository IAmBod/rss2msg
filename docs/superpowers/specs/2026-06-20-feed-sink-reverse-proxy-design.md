# Feed sink behind reverse proxy — design

- **Issue:** [#171](https://github.com/IAmBod/rss2msg/issues/171) — *Feed sink behind reverse proxy*
- **Status:** Approved (brainstorm), ready for implementation plan
- **Date:** 2026-06-20

## Problem

The feed sink serves RSS/Atom/MCP over HTTP and bakes **absolute self-URLs**
(Atom `rel=self`, RSS `atom:link rel=self`) at publisher-init time from
`public_url` (falling back to `link`). It reads **no** `X-Forwarded-*` /
`Forwarded` headers, so behind a reverse proxy an operator must hand-configure a
static `public_url` per instance, the sink cannot honor a proxy-applied path
prefix, and every request appears to originate from the proxy's IP (wrong client
IP in logs and in the issue-#131 auth audit/metrics).

This feature adds opt-in, safe reverse-proxy awareness covering four concerns the
maintainer confirmed are all in scope:

1. Derive self-URLs (scheme/host) from trusted forwarding headers.
2. Honor a proxy-applied path prefix in self-URLs.
3. Recover the real client IP for logging/attribution.
4. A trusted-proxy security model that gates all of the above.

## Decisions (locked during brainstorm)

- **Trust model:** a `trusted_proxies` allowlist of CIDRs plus the presets
  `private` and `all`. Empty/unset = trust nothing.
- **Self-URL precedence:** `public_url` is authoritative and static; forwarding
  headers are used only when `public_url` is unset.
- **Scope:** correct URLs + prefix + client IP + the trust gate. Explicitly *not*
  IP-based authorization, per-surface trust overrides, or rate limiting.

## Design

### 1. Trust model (foundation)

New top-level key under `feed:`:

```yaml
feed:
  trusted_proxies: []   # list of CIDRs and/or presets: `private`, `all`
```

- `private` expands to RFC1918 + loopback + unique-local:
  `10.0.0.0/8`, `172.16.0.0/12`, `192.168.0.0/16`, `127.0.0.0/8`, `::1/128`,
  `fc00::/7`.
- `all` trusts any peer (`0.0.0.0/0` + `::/0`) — an explicit escape hatch.
- Plain CIDR entries (e.g. `10.0.0.0/8`, `127.0.0.1/32`) are accepted alongside
  presets. A bare IP is accepted as a `/32` (`/128` for IPv6).
- **Empty/unset (default) = trust nothing → all forwarding headers ignored →
  output byte-identical to today.** No new attack surface unless opted in.
- A request's forwarding headers are honored only when its **direct peer**
  (`http.Request.RemoteAddr`) is contained in the trusted set.
- Invalid CIDR/preset token is a **config-load error** (validated at startup).

### 2. Self-URL derivation

Self-URLs become **per-request**. Base URL is chosen in this order:

1. `public_url` (static, authoritative) — if set, used verbatim.
2. Trusted forwarding headers (only when the direct peer is trusted).
3. The request `Host` header.
4. `link` (the configured website link) — final fallback.

Then `self = base + [prefix] + surface.path`.

- **Headers parsed** (only from a trusted peer):
  - `Forwarded` (RFC 7239: `proto=`, `host=`). The `for=` parameter is **not**
    consumed — client-IP recovery uses `X-Forwarded-For` only (see §4).
  - `X-Forwarded-Proto`, `X-Forwarded-Host`, `X-Forwarded-Prefix`,
    `X-Forwarded-For`.
  - When both `Forwarded` and an `X-Forwarded-*` header carry the same field,
    `Forwarded` wins. `X-Forwarded-Prefix` is the **only** prefix source
    (RFC 7239 has no prefix parameter).
- **Path prefix** is prepended to the surface path in **self-links only**.
  Internal route matching is unchanged: we assume the proxy strips the prefix
  before forwarding (the standard contract for `X-Forwarded-Prefix`), which the
  docs will state explicitly. No internal route rewriting.
- When `public_url` wins, any header-supplied prefix is ignored (consistent with
  "`public_url` is authoritative"). A `public_url` that already contains a path is
  used as-is.

### 3. Render-cache restructure

Today the rendered RSS/Atom body is cached per surface (`render_cache_ttl`) with
the `rel=self` link baked in. Because self-URLs now vary per request, we split
the two:

- The cached body is rendered **without** the `rel=self` link — identical across
  hosts, so the cache stays effective.
- The self-link is injected **per request** as a cheap string splice. Atom
  already injects `rel=self` as a post-render splice
  (`internal/sink/feed/render.go`); we move that to request time and add the
  equivalent `atom:link rel=self` for RSS.

With `trusted_proxies` empty and `public_url`/`link` configured, the per-request
base resolves to the same value every time, so the emitted output is identical to
the current behavior.

### 4. Real client IP

- Derive by walking the `X-Forwarded-For` chain **right-to-left, skipping
  addresses in the trusted set**; the first untrusted address is the real
  client. Fallbacks: if every hop is trusted, use the left-most entry; if no
  forwarding header (or untrusted peer), use `RemoteAddr`. The RFC 7239
  `Forwarded` header's `for=` parameter is intentionally **not** parsed for
  client IP — only the de-facto `X-Forwarded-For` is. (Documented in
  `docs/how-to/sinks/feed.md`.)
- Surfaced into the **auth-failure log line only** — never a metric attribute.
  Client IP is high-cardinality, so adding it as a metric label would explode
  the time-series cardinality; the auth-failure/auth-success metric attributes
  added in issue #131 stay `surface` + `reason`/`credential` only.
- **No IP-based authorization** is added — client IP is for correct
  logging/attribution only.

## Config, files, and tests

### Config

- Add `TrustedProxies []string` to `FeedSinkConfig` (`internal/config/config.go`),
  `mapstructure:"trusted_proxies"`.
- Validate entries at config load (CIDR / preset parsing).
- Document the key in `docs/how-to/sinks/feed.md`. Note: the example YAML files
  (`internal/config/example.yaml` / `examples/config.example.yaml`) contain no
  feed-sink section, so there is nothing to extend there; the drift-guard test
  still passes unchanged.

### Files touched

| File | Change |
| --- | --- |
| `internal/sink/feed/proxy.go` *(new)* | Trust set, header parsing, client-IP walk, base-URL derivation — pure and unit-testable. |
| `internal/sink/feed/server.go` | Per-request: derive base URL, inject self-link, recompute ETag over the injected body, and log the real client IP on auth failure (log line only, never a metric attribute). |
| `internal/sink/feed/render.go` | Cache body without self-link; per-request injection helper (Atom + RSS). |
| `internal/sink/feed/feed.go` | Stop baking `SelfRSS`/`SelfAtom` at init; build `proxyConfig` and pass it (plus the logger) into the handler. |
| `internal/config/config.go` | `TrustedProxies` field. |
| `internal/config/validate.go` | Validate `trusted_proxies` entries (`validateTrustedProxyEntry`), kept in lockstep with the runtime `parseTrustedProxies`. |
| `cmd/rss2msg/wire.go` | Pass `trusted_proxies` into the publisher options. |
| `docs/how-to/sinks/feed.md` | Rewrite the "TLS vs reverse proxy" section. |

### Tests (TDD)

- `trusted_proxies` parsing: CIDR, `private` preset expansion, `all`, bare IP,
  invalid entry → error.
- `isTrusted(remoteAddr)` matrix across IPv4/IPv6, trusted vs untrusted.
- Client-IP chain walk: mixed trusted/untrusted hops, all-trusted, no XFF,
  untrusted peer.
- Self-URL precedence: `public_url` set vs unset; trusted vs untrusted peer;
  `Forwarded` vs `X-Forwarded-*` precedence; prefix prepend.
- Render: body stable across hosts, `rel=self` varies per request (two requests
  with different `Host` → different self-link, same cached body).
- Integration: feed sink behind a fake proxy that injects forwarding headers.

## Security notes

- Default (empty `trusted_proxies`) changes nothing and adds no attack surface.
- Forwarding headers are never honored from an untrusted direct peer, so a client
  hitting the listener directly cannot spoof its self-URL, host, prefix, or
  client IP.
- `all` is documented as unsafe on a publicly reachable listener.

## Related

- [Issue #171](https://github.com/IAmBod/rss2msg/issues/171)
- [Feed sink auth design (#131)](./2026-06-16-feed-sink-auth-design.md)
- [Feed sink how-to](../../how-to/sinks/feed.md)
