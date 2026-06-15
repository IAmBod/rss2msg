---
title: Get Feeds from Kubernetes
type: how-to
tags: [rss2msg/docs, feeds, kubernetes, dynamic]
summary: Watch Feed custom resources in a Kubernetes cluster and reconcile the serve daemon's feed list from them, with poll-status written back to each CR.
updated: 2026-06-15
---

# Get Feeds from Kubernetes

The `kubernetes` feed source watches `Feed` custom resources (group `rss2msg.io`,
version `v1`) via a dynamic informer and reconciles rss2msg's live feed set from
them. When a `Feed` CR is added, updated, or deleted, the daemon reacts without
a restart. When `write_status` is enabled, rss2msg writes poll outcome back to
each `Feed` CR's `.status` subresource so you can inspect last-poll time, change
count, and errors with `kubectl get feeds`.

## 1. Install the CRD

Apply the bundled CRD manifest:

```bash
kubectl apply -f deploy/crds/feeds.rss2msg.io.yaml
```

If you deploy rss2msg with the Helm chart, set `feedSource.kubernetes.crd.install=true`
(the default) and the chart installs the CRD for you.

## 2. Grant RBAC

The pod's ServiceAccount needs at least `get`, `list`, and `watch` on the
`feeds` resource in the `rss2msg.io` API group. When `write_status: true`, it
also needs `get`, `update`, and `patch` on `feeds/status`.

With the Helm chart, set `feedSource.kubernetes.rbac.create=true` (the default)
and the chart creates a `ClusterRole` and `ClusterRoleBinding` scoped to these
verbs automatically. The `ClusterRole` grants status verbs only when
`feedSource.kubernetes.writeStatus=true`.

To manage RBAC yourself, create a `ClusterRole` equivalent to:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: rss2msg-feeds
rules:
  - apiGroups: ["rss2msg.io"]
    resources: ["feeds"]
    verbs: ["get", "list", "watch"]
  - apiGroups: ["rss2msg.io"]      # add only when write_status: true
    resources: ["feeds/status"]
    verbs: ["get", "update", "patch"]
```

Bind it to the pod's ServiceAccount with a `ClusterRoleBinding`.

## 3. Configure the source

Add a `kubernetes` entry to `feed_sources:` in `config.yaml`:

```yaml
feed_sources:
  - type: kubernetes
    kubernetes:
      namespace: ""          # empty = all namespaces (cluster-wide watch)
      kubeconfig: ""         # empty = in-cluster config (pod's ServiceAccount)
      label_selector: ""     # optional; e.g. "app=myservice"
      resync_interval: 10m   # optional; default 10m; minimum 1s if set
      write_status: true     # optional; default true
```

**`namespace`**: When empty the source watches all namespaces (a cluster-wide
informer). Set to a single namespace to restrict the watch.

**`kubeconfig`**: When empty the source uses the pod's in-cluster credentials
(mounted ServiceAccount token). Set to a kubeconfig file path for local or
out-of-cluster use.

**`label_selector`**: A standard Kubernetes label selector (e.g.
`"env=production,team=platform"`). The informer only lists and watches `Feed`
CRs that match. Validation rejects selectors that fail `labels.Parse` at
startup. Empty means no filtering.

**`resync_interval`**: How often the informer resyncs its cache with the
apiserver. Defaults to `10m`. Must be at least `1s` if set.

**`write_status`**: Whether rss2msg writes poll results back to the `Feed` CR's
`.status` subresource. Defaults to `true`. Set to `false` if the ServiceAccount
has no `feeds/status` permissions.

## 4. Declare a Feed CR

Create `Feed` resources in any namespace the source watches:

```yaml
apiVersion: rss2msg.io/v1
kind: Feed
metadata:
  name: example
  namespace: default
spec:
  url: "https://example.com/feed.xml"   # required
  interval: "5m"                         # optional; Go duration string
  sinks: ["out"]                         # optional; defaults to the "default" sink
  http:                                  # optional HTTP overrides
    timeout: "10s"
    headers:
      X-Token: "abc"
```

The `url` field is required; the CRD schema enforces it. `interval` must be a
Go duration string (e.g. `5m`, `1h30m`). `sinks` is a list of sink names
declared in the rss2msg config. `http.timeout` and `http.headers` are optional.

## 5. Observe status

When `write_status: true`, rss2msg updates `.status` after each poll:

```bash
kubectl get feeds
```

```
NAME      URL                              LAST POLL              CHANGES
example   https://example.com/feed.xml    2026-06-15T10:00:00Z   3
```

The columns — **URL**, **Last Poll**, and **Changes** — come from the CRD's
`additionalPrinterColumns`. Inspect the full status:

```bash
kubectl get feed example -o jsonpath='{.status}'
```

The `.status` fields set by rss2msg:

| field                | type             | description |
| -------------------- | ---------------- | ----------- |
| `observedGeneration` | integer          | The CR generation at the time of the last poll. |
| `lastPollTime`       | RFC 3339 string  | UTC timestamp of the last completed poll. |
| `lastChangeCount`    | integer          | Number of new items detected in the last poll. |
| `lastError`          | string           | Error message from the last poll, or `""` on success. |
| `conditions`         | array            | A single `Ready` condition: `status: "True"` (reason `Polled`) on success, `status: "False"` (reason `PollError`) on error. |

## Related

- [Load Feeds Dynamically](./load-feeds-dynamically.md) — overview of all feed-source types and reload semantics.
- [Configuration Reference](../reference/configuration.md) — top-level config structure and `feed_sources` fields.
