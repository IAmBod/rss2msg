---
title: Azure Service Bus sink
type: how-to
tags: [rss2msg/docs, sinks, azure, servicebus, amqp]
summary: Publish Changes to an Azure Service Bus queue or topic; SAS or Azure AD auth.
updated: 2026-06-01
---

# Azure Service Bus sink

Publishes each `Change` to an Azure Service Bus **queue** or **topic** via the
official `azservicebus` SDK.

```yaml
- name: asb-main
  driver: azureservicebus
  azureservicebus:
    # auth — set exactly one of connection_string or namespace:
    connection_string: ${AZURE_SERVICEBUS_CONNECTION_STRING}   # SAS auth
    # namespace: my-bus.servicebus.windows.net                 # Azure AD (DefaultAzureCredential)
    # entity — set exactly one of queue or topic:
    queue: feed-changes
    # topic: feed-changes
  dead_letter: dlq-main
```

| field               | required | default | notes |
| ------------------- | -------- | ------- | ----- |
| `connection_string` | one-of   | —       | Service Bus SAS connection string. Supports `${ENV}` substitution. Mutually exclusive with `namespace`. |
| `namespace`         | one-of   | —       | Fully-qualified namespace (`<ns>.servicebus.windows.net`). Authenticates with `DefaultAzureCredential` (environment, workload identity, or managed identity). Mutually exclusive with `connection_string`. |
| `queue`             | one-of   | —       | Destination queue name. Mutually exclusive with `topic`. |
| `topic`             | one-of   | —       | Destination topic name. Mutually exclusive with `queue`. |

Exactly one auth field and exactly one entity field must be set; both are
checked at config-validation time and again when the sink is constructed.

Publish layout:

- Body: JSON `Change` envelope.
- `ContentType: application/json`.
- `MessageID = Change.ItemID` (when non-empty) — enables Service Bus duplicate
  detection on entities that have it turned on.
- `ApplicationProperties`: `feed_url`, `kind`, `schema_version`, optional
  `traceparent` / `tracestate` (W3C trace context), and optional
  `dlq_from_sink` / `dlq_error` / `dlq_attempts`.

Implementation notes:

- One `*azservicebus.Client` and one `*azservicebus.Sender` per Publisher. The
  SDK Sender is safe for concurrent use, so publishes are not mutex-serialised.
- A queue and a topic share the same send path; the sink simply targets
  whichever name is configured.
- No auto-reconnect logic of our own: a transient send failure surfaces as a
  publish error and is handled by the sink retry + DLQ layer. The SDK itself
  recovers links/connections on subsequent sends.
- Azure AD auth (`namespace`) uses `DefaultAzureCredential`, which resolves
  credentials from the environment, workload identity, or managed identity in
  that order — no secret needs to live in the config file.
- The integration test (`-tags=integration`) runs against the official Azure
  Service Bus emulator via testcontainers (requires Docker).

## Related

- [Choose a Sink](../choose-a-sink.md) — all drivers and the decision table.
- [Sink Wire Formats](../../reference/wire-formats.md) — on-the-wire layout.
- [Change Envelope](../../reference/change-envelope.md) — the payload body.
