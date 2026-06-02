---
title: gRPC sink
type: how-to
tags: [rss2msg/docs, sinks, grpc]
summary: Deliver each Change to your own gRPC ChangeSink server with a typed protobuf contract, per-RPC deadlines, static metadata, and mTLS.
updated: 2026-06-02
---

# gRPC sink

Delivers each Change to a gRPC server you run that implements the rss2msg
`ChangeSink` service. rss2msg is the **client**: it dials `target` and calls
`Publish` once per detected change. This is the typed analogue of the
[HTTP sink](http.md) — use it when you want a strongly-typed contract,
per-RPC deadlines, HTTP/2 multiplexing, or mutual TLS.

```yaml
- name: grpc-out
  driver: grpc
  grpc:
    target: receiver.internal:50051        # required; gRPC dial target (host:port)
    authority: receiver.example.com        # optional; :authority + TLS server-name override
    timeout: 10s                            # optional; per-RPC deadline (0 -> none)
    metadata:                               # optional; static outgoing metadata per call
      authorization: "Bearer ${GRPC_TOKEN}"
    tls:                                    # optional; omit for plaintext (h2c)
      enabled: true
      ca_file: /etc/rss2msg/ca.pem
      cert_file: /etc/rss2msg/client.pem    # mTLS: set with key_file
      key_file: /etc/rss2msg/client-key.pem
```

| field       | required | default        | notes |
| ----------- | -------- | -------------- | ----- |
| `target`    | yes      | —              | gRPC dial target, typically `host:port`. `${ENV}` substitution works. |
| `authority` | no       | (derived)      | Overrides the `:authority` pseudo-header; also used as the TLS server name when `tls.server_name` is unset. |
| `timeout`   | no       | `0` (none)     | Per-RPC deadline (Go `time.Duration`). The caller's context deadline still applies. |
| `metadata`  | no       | (none)         | Static outgoing metadata attached to every call (e.g. auth). Reserved keys (`:`-prefixed, `grpc-*`, `content-type`) are rejected. Canonical per-change metadata (below) cannot be overridden. |
| `tls`       | no       | (off → h2c)    | Structured client TLS (custom CA / mTLS / verification). When omitted, the connection is plaintext. See [Secure Connections (TLS)](../secure-connections-tls.md#sinks). |

## The contract

The service lives in [`proto/sink/v1/sink.proto`](https://github.com/IAmBod/rss2msg/blob/main/proto/sink/v1/sink.proto)
and ships generated Go stubs you can import to implement a server:

```proto
service ChangeSink {
  rpc Publish(PublishRequest) returns (PublishAck);
}
message PublishRequest { Change change = 1; }
message PublishAck { bool accepted = 1; string error = 2; }
// Change mirrors the Change envelope field-for-field.
```

The `Change` message mirrors the [Change Envelope](../../reference/change-envelope.md):
`*time.Time` fields (`published_at`, `updated_at`) map to absent
`google.protobuf.Timestamp` values; `detected_at` is always present.

## Delivery semantics

- One unary `Publish` RPC per change.
- A non-OK gRPC status **or** an ack with `accepted == false` is treated as a
  delivery failure and drives the configured retry / dead-letter behavior.
- Set `accepted = true` on success. The optional `error` string on a rejecting
  ack is surfaced in rss2msg logs.

## Call layout

- Payload: the typed `Change` protobuf message.
- Canonical per-change metadata: `rss2msg-schema-version`, `rss2msg-feed-url`,
  `rss2msg-item-id`, `rss2msg-kind`, and optional `rss2msg-dlq-from-sink` /
  `rss2msg-dlq-error` / `rss2msg-dlq-attempts`. Set after the static `metadata`
  block so operator keys can't clobber per-change metadata.
- W3C trace context propagates automatically (via the OpenTelemetry gRPC client
  handler) when a span is active.

## Regenerating stubs

Generated `*.pb.go` are committed, so building rss2msg needs no protobuf tooling.
To regenerate after editing the `.proto`, run `task proto` (requires
[`buf`](https://buf.build) plus `protoc-gen-go` and `protoc-gen-go-grpc`).

## Related

- [Choose a Sink](../choose-a-sink.md) — all drivers and the decision table.
- [HTTP sink](http.md) — the JSON-over-HTTP analogue.
- [Secure Connections (TLS)](../secure-connections-tls.md#sinks) — custom CA / mTLS for this sink.
- [Sink Wire Formats](../../reference/wire-formats.md) — on-the-wire layout.
- [Change Envelope](../../reference/change-envelope.md) — the payload fields.
