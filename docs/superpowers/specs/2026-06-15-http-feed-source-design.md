# rss2msg — HTTP Feed Source Design

Status: approved (brainstorming)
Date: 2026-06-15
Builds on: `2026-05-30-dynamic-feed-list.md`
Issue: [#161](https://github.com/IAmBod/rss2msg/issues/161)

## Purpose

Add an `http` feed source: a new `feed_sources` entry that fetches the desired
**feed list** from an HTTP endpoint on an interval. It is the network sibling of
the existing `file` and `postgres` sources — the `FeedSourceConfig.Type` comment
already reserves `http` as a planned type.

Unlike the `file` source (which reads a bare JSON array), the HTTP endpoint
returns a **JSON object** with the feed list under a `feeds` key. The wrapper
object leaves room for forward-compatible metadata (versioning, generation
timestamps, pagination) without a breaking format change, and distinguishes an
intentionally empty list from a malformed body.

The source must support authenticating to the list endpoint. Per the existing
client-side HTTP convention in this repo (the HTTP sink and per-feed
`FeedHTTPConfig` both authenticate via a `headers` map plus a `tls` block, with
no structured `auth` block), all auth flows are expressed that way:

- **Bearer token** — `Authorization: Bearer ${TOKEN}` via `headers`.
- **Basic auth** — `Authorization: Basic <base64(user:pass)>` via `headers`.
- **API key / custom schemes** — e.g. `X-API-Key: ${KEY}` via `headers`.
- **mTLS** — client cert + custom CA via the `tls` block.

Out of scope: per-feed RSS/Atom fetch auth (extending `FeedHTTPConfig` with
structured auth) — that is a separate concern noted as a possible follow-up.

## Config schema

A new `http` block on `FeedSourceConfig`, parallel to the existing `postgres`
block. `interval` reuses the existing top-level `FeedSourceConfig.Interval`
(1s minimum, enforced by current validation).

```yaml
feed_sources:
  - type: http
    name: control-plane          # optional; defaults to "http[<index>]"
    interval: 30s                 # poll cadence (reuses FeedSourceConfig.Interval)
    http:
      url: https://cp.example/feeds        # required; ${ENV} expands
      timeout: 10s                          # per-request; default 30s
      headers:                              # arbitrary request headers
        Authorization: "Bearer ${CP_TOKEN}"
        X-API-Key: "${CP_API_KEY}"
      tls:                                  # optional; mirrors FeedSourcePGTLSConfig
        ca_file: /etc/ssl/cp-ca.pem
        cert_file: /etc/ssl/client.pem      # cert_file + key_file: both or neither
        key_file: /etc/ssl/client-key.pem
        server_name: cp.internal
        insecure_skip_verify: false
```

### Response payload

The endpoint returns a JSON object whose `feeds` key holds an array of the same
`FeedSpec` shape the `file` source reads. JSON only in v1.

```json
{
  "feeds": [
    { "url": "https://example.com/blog/rss.xml", "interval": "5m", "sinks": ["pg-main"] },
    { "url": "https://other.example/atom.xml", "http": { "headers": { "Authorization": "Bearer ..." } } }
  ]
}
```

A response missing the `feeds` key, or whose `feeds` value is not an array, is a
decode error **and is logged at warn** — see [Observability](#observability)
below. An empty `feeds` array (`{"feeds": []}`) is valid and yields an empty list
for this source. The `feeds` array is decoded into `[]FeedSpec` and passed
through the existing `SpecsToConfigs`. The key is fixed (`feeds`) in v1, not
configurable.

To keep "missing key" distinguishable from "empty array", decode into a wrapper
with a pointer slice:

```go
// feedListResponse is the wire shape the http source expects.
type feedListResponse struct {
    Feeds *[]FeedSpec `json:"feeds"`
}
```

A nil `Feeds` (key absent) is an error; a non-nil empty slice is a valid empty
list.

### New config structs (`internal/config/config.go`)

```go
type HTTPFeedSourceConfig struct {
    URL     string                   `mapstructure:"url"`     // required
    Timeout time.Duration            `mapstructure:"timeout"` // default 30s
    Headers map[string]string        `mapstructure:"headers"`
    TLS     FeedSourceHTTPTLSConfig  `mapstructure:"tls"`
}

// Identical field set to FeedSourcePGTLSConfig (kept as its own type for the
// http namespace; no shared abstraction introduced — matches per-sink pattern).
type FeedSourceHTTPTLSConfig struct {
    CAFile             string `mapstructure:"ca_file"`
    CertFile           string `mapstructure:"cert_file"`
    KeyFile            string `mapstructure:"key_file"`
    ServerName         string `mapstructure:"server_name"`
    InsecureSkipVerify bool   `mapstructure:"insecure_skip_verify"`
}
```

`FeedSourceConfig` gains an `HTTP HTTPFeedSourceConfig` field with the
`mapstructure:"http"` tag.

## Implementation (`internal/feedsource/http.go`)

`NewHTTP(opts HTTPOptions) (*HTTP, error)` builds an `*http.Client` once
(timeout + TLS config) and returns a source backed by the existing `Poll`
helper — the same primitive the `postgres` source uses. `Poll` ticks every
`interval` and delegates reads to a `FetchFunc`; the aggregator's per-source
last-known-good already absorbs transient fetch errors.

```go
type HTTPOptions struct {
    Name     string
    URL      string
    Timeout  time.Duration       // <=0 → 30s
    Headers  map[string]string
    Interval time.Duration
    TLS      *HTTPTLSOptions     // nil when no tls block configured
}
```

The TLS config is built with the same logic as the HTTP sink's
`buildTLSConfig` (`internal/sink/http/http.go`): optional custom CA pool,
optional client cert (`cert_file`/`key_file` both-or-neither),
`server_name` override, `insecure_skip_verify`. Reuse/extract rather than
re-derive.

### Fetch + conditional GET (ETag / Last-Modified)

The `FetchFunc` closure holds cache state behind a `sync.Mutex`: the last seen
`ETag`, the last `Last-Modified`, and the last successfully decoded
`[]config.FeedConfig`. This mirrors the conditional-GET pattern already present
in `internal/feed/fetcher.go` (`FetchRequest`/`FetchResult`).

Per tick:

1. Build `GET url`; apply `headers`; if a cached `ETag` exists set
   `If-None-Match`, if a cached `Last-Modified` exists set `If-Modified-Since`.
2. On `304 Not Modified` → return the cached feed list unchanged (no decode).
3. On `2xx` → decode the JSON object into `feedListResponse`, validate the
   `feeds` key is present (non-nil), convert via `SpecsToConfigs`; store the
   response's `ETag`/`Last-Modified` and the decoded list as the new cache;
   return it.
4. On non-2xx/non-304, transport error, malformed JSON, or a missing `feeds`
   key → return an error. `Poll` forwards it; the aggregator keeps the
   last-known-good list. A `2xx` whose body is missing the `feeds` key is
   additionally logged at warn (see [Observability](#observability)).

### Observability

The aggregator (`internal/feedsource/aggregator.go`) deliberately swallows a
source's fetch error and falls back to that source's last-known-good list
**without logging** — so a misconfigured endpoint (wrong URL, an API returning a
different JSON shape) would otherwise silently serve stale feeds. To make that
case diagnosable, the http source logs a warn itself when a `2xx` response
decodes but lacks the `feeds` key, using the package zerolog logger and the same
field convention as the postgres source (`postgres.go`):

```go
log.Warn().
    Str("component", "feedsource/http").
    Str("source", name).
    Str("url", url).
    Msg("http feed source: response missing \"feeds\" key; keeping last-known-good")
```

Scope note: only the missing-`feeds`-key case is logged in v1 (it is the most
common, most diagnosable misconfiguration). Broader per-error logging at the
aggregator level is out of scope for this change.

`Poll` still signals `Changes()` on every tick regardless of content; on a 304
the returned list is byte-identical to the prior read, so the aggregator dedup
and downstream reconciliation no-op. ETag/`If-Modified-Since` is therefore a
bandwidth/parse optimization, not a correctness mechanism — but it is included
in v1 per the design decision.

`HTTP` satisfies `Source` through the embedded `Poll`; `Close()` stops the
poller. A compile-time `var _ Source = (*HTTP)(nil)` assertion is included.

## Wiring (`cmd/rss2msg/sources.go`)

Add `case "http"` to `buildSources`: assemble `HTTPOptions` from
`sc.HTTP` and `sc.Interval`, populate `TLS` only when the `tls` block is
non-zero (matching the postgres `if sc.Postgres.TLS != (…){}` guard), call
`feedsource.NewHTTP`, and register the closer.

## Validation (`internal/config/validate.go`)

Add `"http"` to the feed_sources type switch (and to the
"unsupported type" allow-list). Rules:

- `http.url` is required → error if empty.
- `cert_file`/`key_file` must be both set or both empty.
- `interval` already validated by the shared 1s-minimum check.

No mutual-exclusivity rules are needed (no structured auth block; headers are
free-form).

## Error handling summary

| Condition | Behavior |
| --- | --- |
| `2xx`, object with `feeds` array | Decode, cache, return new list |
| `304 Not Modified` | Return cached list (no decode) |
| `{"feeds": []}` | Valid: empty list for this source |
| `feeds` key missing / not an array | Warn-logged + error → aggregator keeps last-known-good |
| non-2xx (e.g. 401/500) | Error → aggregator keeps last-known-good |
| transport error / timeout | Error → aggregator keeps last-known-good |
| malformed JSON | Error → aggregator keeps last-known-good |

## Testing

TDD with `net/http/httptest` (no Docker required):

- Each auth flow reaches the server: bearer header, basic header, custom
  `X-API-Key` header.
- mTLS round-trip via `httptest.NewTLSServer` + a client cert / custom CA.
- Conditional GET: first `200` caches the `ETag`; a follow-up tick sends
  `If-None-Match` and the server replies `304` → cached list returned, body not
  re-parsed.
- `200` with a changed `ETag` → new list adopted.
- Payload shape: object with `feeds` array decodes; `{"feeds": []}` yields an
  empty list; a missing `feeds` key and a bare array `[...]` each error, and the
  missing-key case emits a warn log (assert via a zerolog test writer / captured
  output).
- Non-2xx, transport error, and malformed JSON each surface as errors.
- `Close()` stops the poller.
- Config: validation accepts a well-formed `http` block; rejects missing `url`
  and a lone `cert_file`/`key_file`.

`task test`, `task vet`, `task lint` before the PR. No integration suite needed
(httptest is in-process); say so explicitly in the PR.

## Docs

- Extend `docs/how-to/load-feeds-dynamically.md` with an HTTP-source section
  (config example + auth-via-headers note), linking to
  `docs/how-to/secure-connections-tls.md` for mTLS.
- Update `docs/reference/configuration.md` with the new fields.
- Add a commented `http` entry to `internal/config/example.yaml`.
- Run `bash scripts/check-doc-links.sh`.

## Files touched

- `internal/feedsource/http.go` (new) + `internal/feedsource/http_test.go` (new)
- `internal/config/config.go` (structs)
- `internal/config/validate.go` (+ `validate_test.go`)
- `cmd/rss2msg/sources.go` (wiring)
- `docs/how-to/load-feeds-dynamically.md`, `docs/reference/configuration.md`,
  `internal/config/example.yaml`
