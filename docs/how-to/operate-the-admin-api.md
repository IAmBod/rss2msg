---
title: Operate the Admin API
type: how-to
tags: [rss2msg/docs, admin-api, operations, security]
summary: Enable the admin HTTP API, configure token auth and mTLS, and securely access the introspection and maintenance endpoints.
updated: 2026-06-22
---

# Operate the Admin API

The `serve` daemon includes an opt-in admin HTTP API that exposes JSON
introspection (status, feeds, members, health) and safe maintenance actions
(reconcile, per-feed poll, state prune). It runs on a dedicated listener
separate from the health probe port and is **disabled by default**.

## Enable the admin API

Add the `admin:` block to your config and set `enabled: true`:

```yaml
admin:
  enabled: true
  listen: ":8090"
  auth:
    enabled: true
    bearer_tokens:
      - name: ops
        token: "${ADMIN_TOKEN}"
```

Set `ADMIN_TOKEN` to a strong random value (e.g. `openssl rand -hex 32`). The
`listen` address is required when `enabled: true`.

## Configure token authentication

`auth.enabled` defaults to `true`. Auth is enforced only when `auth.enabled: true`
**and** at least one credential is configured. If you enable auth but provide no
credentials, config validation fails and the daemon refuses to start — this is
the secure-by-default guard that prevents accidentally running an open API with the
default setting still in place. To make the API fully open you must explicitly set
`auth.enabled: false` (see [Disabling auth](#disabling-auth)); the middleware then
becomes a pass-through and config validation warns that the API is open unless mTLS
is also configured.

Three credential methods are supported; any combination may be configured:

### Bearer tokens

```yaml
admin:
  auth:
    bearer_tokens:
      - name: ops
        token: "${ADMIN_TOKEN}"
      - name: readonly-ci
        token: "${CI_ADMIN_TOKEN}"
```

Send requests with:

```bash
curl -s http://localhost:8090/v1/status \
  -H "Authorization: Bearer ${ADMIN_TOKEN}"
```

### HTTP Basic auth

```yaml
admin:
  auth:
    basic_users:
      - name: ops
        username: admin
        password: "${ADMIN_PASSWORD}"
```

```bash
curl -s http://localhost:8090/v1/feeds \
  -u "admin:${ADMIN_PASSWORD}"
```

### API key header

```yaml
admin:
  auth:
    api_key_header: X-API-Key
    api_keys:
      - name: ci
        key: "${ADMIN_API_KEY}"
```

```bash
curl -s http://localhost:8090/v1/status \
  -H "X-API-Key: ${ADMIN_API_KEY}"
```

## Configure mTLS

mTLS is an independent layer from application auth and can be used alone or
alongside bearer/basic/api-key. When `tls.client_ca_file` is set, the TLS
handshake requires a client certificate signed by that CA. See
[Secure Connections (TLS)](./secure-connections-tls.md) for certificate
generation and management.

```yaml
admin:
  tls:
    enabled: true
    cert_file: /etc/ssl/admin/server.pem
    key_file:  /etc/ssl/admin/server-key.pem
    client_ca_file: /etc/ssl/admin/client-ca.pem
  auth:
    enabled: true
    bearer_tokens:
      - name: ops
        token: "${ADMIN_TOKEN}"
```

With mTLS enabled, clients must present a valid certificate:

```bash
curl -s https://localhost:8090/v1/status \
  --cacert /etc/ssl/admin/client-ca.pem \
  --cert   /etc/ssl/admin/client.pem \
  --key    /etc/ssl/admin/client-key.pem \
  -H "Authorization: Bearer ${ADMIN_TOKEN}"
```

## curl examples

These examples assume `auth.enabled: true` with a bearer token and the default
`listen: ":8090"`.

### Get daemon status

```bash
curl -s http://localhost:8090/v1/status \
  -H "Authorization: Bearer ${ADMIN_TOKEN}" | jq .
```

### List feeds

```bash
curl -s http://localhost:8090/v1/feeds \
  -H "Authorization: Bearer ${ADMIN_TOKEN}" | jq .
```

### Get a single feed by URL

The feed URL must be percent-encoded — encode every `/` as `%2F`, `:` as `%3A`:

```bash
FEED_ID=$(python3 -c "import urllib.parse; print(urllib.parse.quote('https://example.com/blog/rss.xml', safe=''))")
curl -s "http://localhost:8090/v1/feeds/${FEED_ID}" \
  -H "Authorization: Bearer ${ADMIN_TOKEN}" | jq .
```

### Trigger feed reconciliation

```bash
curl -s -X POST http://localhost:8090/v1/feeds/reconcile \
  -H "Authorization: Bearer ${ADMIN_TOKEN}" | jq .
```

### Trigger an immediate poll for one feed

```bash
FEED_ID=$(python3 -c "import urllib.parse; print(urllib.parse.quote('https://example.com/blog/rss.xml', safe=''))")
curl -s -X POST "http://localhost:8090/v1/feeds/${FEED_ID}/poll" \
  -H "Authorization: Bearer ${ADMIN_TOKEN}" | jq .
```

### Prune state store

Remove items and feed metadata older than 30 days (720 hours):

```bash
curl -s -X POST http://localhost:8090/v1/state/prune \
  -H "Authorization: Bearer ${ADMIN_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"items_older_than":"720h","feed_meta_older_than":"720h"}' | jq .
```

### Check admin health

```bash
curl -s http://localhost:8090/v1/health \
  -H "Authorization: Bearer ${ADMIN_TOKEN}" | jq .
```

## Security guidance

**Bind privately.** Use a non-routable address (e.g. `listen: "127.0.0.1:8090"`) or
an internal network interface. Never expose the admin port on a public-facing
interface without additional network controls (firewall, VPN, or similar).

**Keep `auth.enabled: true`.** This is the default. The admin API exposes
operational details (feed URLs, tokens in config comments are not a concern,
but ownership maps and build metadata are) and allows write actions
(reconcile, poll, prune). Always configure at least one credential method.

**Use mTLS for automated access.** For CI pipelines or monitoring systems that
call the admin API, mTLS provides mutual authentication without shared secrets
in environment variables. See [Secure Connections (TLS)](./secure-connections-tls.md).

**Set `auth.enabled: false` only deliberately.** This disables all application-
level authentication. Only appropriate when the admin listener is bound exclusively
to localhost (`127.0.0.1`) or is protected by network-level controls you fully
control. Document the decision in your deployment config.

**Rotate tokens.** Bearer tokens and API keys are read from environment variables
(`${ADMIN_TOKEN}`). Rotate by updating the secret and restarting (or sending
`SIGHUP` for a config reload if your secret manager supports it).

## Disabling auth

To explicitly disable application-level auth (loopback-only deployments):

```yaml
admin:
  enabled: true
  listen: "127.0.0.1:8090"
  auth:
    enabled: false
```

This is intentionally verbose — you must set `auth.enabled: false` explicitly;
there is no way to arrive at no-auth by accident.

## Related

- [Admin API Reference](../reference/admin-api.md) — endpoint reference, request/response schemas.
- [Configuration Reference](../reference/configuration.md) — the full `admin:` config block.
- [Secure Connections (TLS)](./secure-connections-tls.md) — certificate setup and mTLS.
- [Configure Kubernetes Health Probes](./configure-kubernetes-health-probes.md) — the separate health probe listener.
