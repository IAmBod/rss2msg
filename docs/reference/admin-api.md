---
title: Admin API Reference
type: reference
tags: [rss2msg/docs, admin-api, operations]
summary: Endpoint reference for the opt-in admin HTTP API — introspection and safe maintenance actions for the serve daemon.
updated: 2026-06-22
---

# Admin API Reference

The admin API is an opt-in HTTP server that exposes JSON introspection and safe
maintenance actions for a running `serve` daemon. It is **disabled by default**.
Enable it with `admin.enabled: true` in the config.

All endpoints are under the path prefix `/v1/`. Requests and responses use
`Content-Type: application/json`.

See [Operate the Admin API](../how-to/operate-the-admin-api.md) for setup,
auth configuration, `curl` examples, and security guidance.

## Endpoints

### `GET /v1/status`

Returns build information and a live snapshot of the daemon's state.

**Response `200 OK`:**

```json
{
  "instance_id":        "hostname-or-configured-id",
  "version":            "v1.2.3",
  "commit":             "abc1234",
  "date":               "2026-06-22T10:00:00Z",
  "uptime_seconds":     3600,
  "assignment_enabled": false,
  "feed_count":         4,
  "member_count":       1
}
```

| field                | notes |
| -------------------- | ----- |
| `instance_id`        | The instance's identity string (from `telemetry.instance_id` or the hostname). |
| `version`            | Build version injected at compile time via `-ldflags`. |
| `commit`             | Short commit SHA injected at compile time. |
| `date`               | Build date injected at compile time. |
| `uptime_seconds`     | Seconds since the daemon started. |
| `assignment_enabled` | `true` when `coordination.assignment.enabled` is set and a `MembershipInspector` is wired. |
| `feed_count`         | Number of feeds in the current desired set. |
| `member_count`       | Number of known cluster members (1 in single-instance mode). |

---

### `GET /v1/feeds`

Lists all feeds in the desired set with their current HTTP cache state.

**Response `200 OK`:**

```json
{
  "feeds": [
    {
      "url":              "https://example.com/blog/rss.xml",
      "interval_seconds": 300,
      "owned":            true,
      "etag":             "\"abc123\"",
      "last_modified":    "2026-06-21T08:00:00Z"
    }
  ],
  "total": 1
}
```

| field              | type            | notes |
| ------------------ | --------------- | ----- |
| `url`              | string          | The feed URL as configured. |
| `interval_seconds` | float           | Poll interval in seconds. |
| `owned`            | bool            | `true` if this instance is currently responsible for polling this feed (always `true` in single-instance mode). |
| `etag`             | string or null  | The `ETag` header value from the feed's last successful HTTP response, if the feed returned one. |
| `last_modified`    | string or null  | The `Last-Modified` header value (RFC 3339, UTC) from the feed's last successful HTTP response, if the feed returned one. |

`etag` and `last_modified` are the HTTP cache validators stored in the state store. They
are **not** a last-polled timestamp; they reflect what the upstream feed server last
returned so the daemon can send `If-None-Match` / `If-Modified-Since` on the next poll.

**Status codes:** `200` on success; `500` if the feed list cannot be loaded.

---

### `GET /v1/feeds/{id}`

Returns a single feed. `{id}` is the **percent-encoded feed URL**. Clients must
URL-encode the full feed URL, encoding `/` as `%2F`. A client that encodes the
scheme separator (`://` → `%3A%2F%2F`) but leaves path slashes (`/`) literal will
receive `404`.

Example: `https://example.com/blog/rss.xml` → encode as
`https%3A%2F%2Fexample.com%2Fblog%2Frss.xml`.

**Response `200 OK`:** the same `feedView` object as a single element of the `feeds`
array in `GET /v1/feeds`.

**Status codes:**
- `200` — feed found.
- `400` — `{id}` cannot be URL-decoded.
- `404` — no feed with that URL in the desired set.
- `500` — feed list cannot be loaded.

---

### `GET /v1/members`

Returns the cluster membership view and the per-feed ownership map.

**Response `200 OK`:**

```json
{
  "self":      "instance-a",
  "members":   ["instance-a", "instance-b"],
  "ownership": {
    "https://example.com/blog/rss.xml": "instance-a",
    "https://other.example/atom.xml":   "instance-b"
  }
}
```

| field       | notes |
| ----------- | ----- |
| `self`      | This instance's identity string. |
| `members`   | All currently known members. In single-instance mode, contains only `self`. |
| `ownership` | Map of feed URL to the member that owns it according to rendezvous-hash assignment. |

**Status codes:** `200` on success.

---

### `GET /v1/health`

Runs all registered dependency checks and returns their status. This endpoint is
equivalent to the readiness probe but accessible via the admin listener.

**Response `200 OK` (all checks pass):**

```json
{
  "ok":     true,
  "checks": {
    "state":        "ok",
    "coordination": "ok"
  }
}
```

**Response `503 Service Unavailable` (any check fails):**

```json
{
  "ok":     false,
  "checks": {
    "state":        "dial tcp: connection refused",
    "coordination": "ok"
  }
}
```

**Status codes:** `200` when all checks pass; `503` when any check fails.

---

### `POST /v1/feeds/reconcile`

Triggers an immediate feed-list reconciliation (the same as sending `SIGHUP`). The
scheduler reloads all dynamic feed sources and adjusts its internal ticker set
without a restart.

**Request body:** none.

**Response `202 Accepted`:**

```json
{"status": "reconcile triggered"}
```

**Status codes:**
- `202` — reconcile enqueued.
- `503` — reconcile is not available (reconcile hook not wired).

---

### `POST /v1/feeds/{id}/poll`

Triggers an immediate out-of-schedule poll for a single feed. `{id}` is the
percent-encoded feed URL (same encoding rules as `GET /v1/feeds/{id}`).

**Request body:** none.

**Response `202 Accepted`:**

```json
{
  "status":  "poll triggered",
  "running": false
}
```

`running` is `true` if the feed was already mid-poll when the request arrived
(the new poll still runs once the current one completes, subject to scheduler
behaviour).

**Status codes:**
- `202` — poll triggered.
- `400` — `{id}` cannot be URL-decoded.
- `404` — no feed with that URL in the desired set.
- `500` — feed list cannot be loaded.

---

### `POST /v1/state/prune`

Prunes expired items and feed metadata from the state store. Runs synchronously;
the response body reports counts of removed rows.

**Request body** (optional):

```json
{
  "items_older_than":     "720h",
  "feed_meta_older_than": "720h"
}
```

Both fields are Go duration strings (e.g. `"720h"`, `"30d"` is not valid — use
`"720h"`). If a field is omitted or the body is absent entirely, the configured
`state.item_ttl` is used as the default for that field.

**Response `200 OK`:**

```json
{
  "items_removed":     42,
  "feed_meta_removed": 3
}
```

**Status codes:**
- `200` — prune complete.
- `400` — a duration string is malformed.
- `500` — the state store returned an error.

---

## Authentication

The admin API enforces application-level authentication independently of TLS.
`auth.enabled` defaults to `true`. Configure at least one credential method
before enabling the server; the auth middleware rejects unauthenticated requests
with `401 Unauthorized` and a `WWW-Authenticate` challenge header.

### Bearer token

```http
Authorization: Bearer <token>
```

Configure in `admin.auth.bearer_tokens`:

```yaml
admin:
  auth:
    bearer_tokens:
      - name: ops
        token: "${ADMIN_TOKEN}"
```

### HTTP Basic

```http
Authorization: Basic <base64(username:password)>
```

Configure in `admin.auth.basic_users`:

```yaml
admin:
  auth:
    basic_users:
      - name: ops
        username: admin
        password: "${ADMIN_PASSWORD}"
```

### API key (header)

```http
X-API-Key: <key>
```

The header name is configurable via `admin.auth.api_key_header` (default
`X-API-Key`). Configure keys in `admin.auth.api_keys`:

```yaml
admin:
  auth:
    api_key_header: X-API-Key
    api_keys:
      - name: ci
        key: "${ADMIN_API_KEY}"
```

### Mutual TLS (mTLS)

mTLS is an independent transport-layer control. Set `admin.tls.client_ca_file`
to the path of the CA that signed your client certificates:

```yaml
admin:
  tls:
    enabled: true
    cert_file: /etc/ssl/admin/server.pem
    key_file:  /etc/ssl/admin/server-key.pem
    client_ca_file: /etc/ssl/admin/client-ca.pem
```

When `client_ca_file` is set, the TLS handshake requires a valid client
certificate signed by that CA (`tls.RequireAndVerifyClientCert`). mTLS and
application auth (`auth.enabled: true`) can be combined; both must pass.

### Disabling auth

Set `auth.enabled: false` only in isolated, network-restricted environments (e.g.
localhost-only loopback binding). See [Operate the Admin API](../how-to/operate-the-admin-api.md)
for security guidance.

---

## CORS

When `admin.cors.allowed_origins` is non-empty, the server adds CORS response
headers for matching origins and handles `OPTIONS` preflight requests. An empty
list (the default) disables CORS entirely.

Allowed methods: `GET`, `POST`, `OPTIONS`.
Allowed request headers: `Authorization`, `Content-Type`, `X-API-Key`.

```yaml
admin:
  cors:
    allowed_origins: ["https://ops.example.com"]
```

Use `"*"` to allow any origin (not recommended in production).

## Related

- [Operate the Admin API](../how-to/operate-the-admin-api.md) — enable, configure, curl examples.
- [Configuration Reference](configuration.md) — the full `admin:` config block.
- [Secure Connections (TLS)](../how-to/secure-connections-tls.md) — certificate setup.
- [Configure Kubernetes Health Probes](../how-to/configure-kubernetes-health-probes.md) — the separate health probe listener.
