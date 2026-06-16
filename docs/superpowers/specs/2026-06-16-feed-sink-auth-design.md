# rss2msg — Feed Sink Auth Hardening Design

Status: approved (brainstorming)
Date: 2026-06-16
Builds on: `internal/sink/feed/auth.go` (basic/bearer auth shipped in `8b11486`)
Issue: [#131](https://github.com/IAmBod/rss2msg/issues/131)

## Purpose

The feed sink serves items over HTTP as RSS, Atom, and MCP surfaces. As of
commit `8b11486` it has **optional inbound auth**, but the model is deliberately
minimal: exactly **one** credential (one basic user *or* one bearer token),
applied **uniformly to every surface**.

This design replaces that with a richer auth model supporting:

- **Multiple labeled credentials** — lists of basic users, bearer tokens, and
  API keys, each with an optional `name` for observability and rotation.
- **API-key header auth** — a key checked against a configurable request header
  (default `X-API-Key`), alongside `Authorization` basic/bearer.
- **mTLS client certificates** — verify consumer client certs against a
  sink-wide CA pool, enforced per-surface.
- **Per-surface rules** — RSS public while MCP requires a client cert + bearer,
  etc. Currently auth is all-or-nothing across all surfaces.

There are **no existing users** of rss2msg, so the config schema is changed
freely: the current `auth.basic` / `auth.bearer_token` shape is **removed**, not
deprecated.

## Config schema

Top-level `feed.auth:` is the **default** applied to every enabled surface. A
surface's own `auth:` block **fully replaces** the default (no field-level
merge — replace-not-merge keeps resolution trivial to reason about). A surface
with `auth: {disabled: true}` is explicitly public.

```yaml
sinks:
  - name: myfeed
    driver: feed
    feed:
      listen: "0.0.0.0:8443"
      tls:                                  # server TLS (existing)
        cert_file: /etc/rss2msg/server.pem
        key_file:  /etc/rss2msg/server-key.pem
      mtls_ca_file: /etc/rss2msg/clients-ca.pem   # sink-wide CA pool for client certs

      auth:                                 # DEFAULT for all surfaces
        basic_users:
          - {name: alice, username: alice, password: s3cret}
        bearer_tokens:
          - {name: ci-bot, token: tok_a}
          - {name: mobile, token: tok_b}
        api_keys:
          - {name: partner-x, key: key_1}
        api_key_header: X-API-Key           # default when omitted
        mtls: {require: true}               # require a verified client cert

      rss:  {enabled: true, auth: {disabled: true}}    # PUBLIC override
      atom: {enabled: true}                            # inherits the default
      mcp:
        enabled: true
        auth:                               # full override
          bearer_tokens: [{name: mcp, token: t_mcp}]
          mtls: {require: true}
```

### mTLS split (refinement)

mTLS is transport-level and there is a single TLS listener per feed sink, so the
CA pool is **sink-wide** (`feed.mtls_ca_file`), while **enforcement is
per-surface** (`auth.mtls.require: bool`).

- The listener uses `tls.VerifyClientCertIfGiven` **whenever** `mtls_ca_file` is
  set: a presented client cert is verified against the CA pool, but a cert is
  not demanded at the handshake (so public surfaces still work on the same
  port).
- `auth.mtls.require: true` on a surface enforces that a **verified** client
  cert was presented for requests to that surface.
- The earlier `mode: off|optional|require` trichotomy is dropped in favor of the
  sink-wide CA + per-surface boolean `require`.

## Evaluation semantics

Per request, against the **effective** auth block for the matched surface
(surface override → top-level default → public):

1. If the block is `disabled` or defines **no methods** → **allow** (public).
2. **mTLS gate** — if `mtls.require`, the request must carry a verified client
   cert (`req.TLS != nil && len(req.TLS.PeerCertificates) > 0`; the listener has
   already verified it against the CA). Missing/invalid → reject.
3. **Token check** — if any token methods are configured (`basic_users`,
   `bearer_tokens`, `api_keys`), the request must match **one** of them. This is
   **OR** across credential types and across entries within a type:
   - `Authorization: Basic` → matched against `basic_users`.
   - `Authorization: Bearer` → matched against `bearer_tokens`.
   - `api_key_header` value → matched against `api_keys`.
4. **mTLS AND tokens** — when a surface configures both an mTLS requirement and
   token methods, **both** must pass (defense in depth). A surface that requires
   only mTLS (no tokens) is satisfied by a valid client cert alone.
5. All secret comparisons are constant-time (`crypto/subtle.ConstantTimeCompare`,
   reusing the existing `ctEqual` helper).

### Failure responses

- Missing/invalid **client cert** on an mTLS-required surface → **401** with an
  explanatory body (chosen over 403 for consistency with the token path).
- Missing/invalid **token** → **401** with the existing `WWW-Authenticate`
  challenge (`Basic` when basic users are configured; otherwise `Bearer`).

## Observability

- On success, the matched credential's `name` (or the client-cert subject CN for
  an mTLS-only match) is recorded:
  - a debug log line (`authenticated`, with `surface` and `credential`);
  - `feed_auth_success_total{surface,credential}` counter. Cardinality is bounded
    by operator-defined names, so this is safe as a metric label.
- On failure: `feed_auth_failure_total{surface,reason}` where `reason` ∈
  `{no_credentials, bad_token, no_client_cert}`.
- `Cache-Control: private` continues to be set on any authenticated (non-public)
  response, as today.

## Validation (`internal/config/validate.go`)

Replaces the current "only one of basic or bearer" rule. New rules, evaluated for
the top-level default block and every surface override:

- `mtls.require: true` anywhere (default or any surface) but `feed.mtls_ca_file`
  unset → error.
- A surface `auth` block that is **non-empty, not `disabled`, and defines zero
  methods** → error (prevents accidental lock-open / lock-out from a typo).
- `disabled: true` combined with any method in the same block → error.
- `basic_users` entries require **both** `username` and `password`.
- `bearer_tokens` entries require a non-empty `token`; `api_keys` entries require
  a non-empty `key`.
- Duplicate `name`s within a single credential type → error (names must be
  unambiguous for metrics/logs).
- `api_key_header`, if set, must be a valid HTTP header token; default
  `X-API-Key` when omitted but `api_keys` are present.

## Testing (TDD)

Table-driven handler tests, one matrix per surface:

- public (no block / inherited / `disabled: true`);
- single and multiple `bearer_tokens`; `basic_users`; `api_keys` with both the
  default and a custom `api_key_header`;
- mTLS-required: request with a valid client cert, with no cert, with a cert
  signed by an untrusted CA;
- mTLS **AND** token combinations (cert-only fails when tokens also required;
  cert + valid token passes);
- inheritance vs. override vs. `disabled`;
- every validation error case above (unit tests in `internal/config`).

mTLS tests build an in-test CA, a server cert, and a client cert, and drive an
`httptest.NewUnstartedServer` configured with the sink's `tls.Config`
(`VerifyClientCertIfGiven` + the CA pool). Metrics assertions verify the
`credential` label carries the matched name.

## Delivery plan

Implemented as **two PRs**, each on its own branch in its own worktree (per
`AGENTS.md`), with the full spec consolidated in the issue body:

- **PR-A — auth-model redesign (token methods).** Labeled multi-credential lists
  (`basic_users` / `bearer_tokens` / `api_keys`), API-key header support,
  per-surface override + `disabled`, the evaluation pipeline (built AND-ready so
  PR-B slots in without rework), token validation rules, and success/failure
  metrics. The `mtls` config keys are **not** introduced in PR-A — neither
  parsed nor validated — so PR-A never ships a knob it can't honor.
- **PR-B — mTLS.** Adds `feed.mtls_ca_file` and the per-surface `auth.mtls`
  block, the listener `VerifyClientCertIfGiven` wiring, the mTLS validation rules
  (`require` needs a CA), `mtls.require` enforcement plugged into PR-A's
  evaluation pipeline as the gate before the token check, and the mTLS test
  matrix.

PR-B depends on PR-A and lands after it. Final sequencing and any further
slicing is a writing-plans concern.

## Docs

- Update the feed-sink reference page under `docs/reference/` (and any feed-sink
  how-to) with the new auth schema and the mTLS section.
- Update **both** `examples/config.example.yaml` and
  `internal/config/example.yaml` — they must stay **byte-identical** (a
  drift-guard test enforces this).
- Run `bash scripts/check-doc-links.sh` (must print
  `OK: all relative doc links resolve`).

## Out of scope

- OAuth2 / OIDC / JWT verification (token introspection, issuer/audience checks).
- Rate limiting and IP allow-lists.
- Auth on the health/metrics servers (separate concern, separate issue).
- Secret references / external secret managers (credentials are inline config or
  `${ENV}` strings, per existing config conventions).
