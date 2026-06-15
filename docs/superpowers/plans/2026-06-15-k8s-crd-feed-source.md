# K8s CRD Feed Source Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a Kubernetes-native feed source (`type: kubernetes`) that watches `Feed` custom resources via a dynamic informer, reconciles rss2msg's feed set from them, and writes poll status back to each CR's `.status` subresource.

**Architecture:** A fourth `feedsource.Source` implementation backed by `client-go`'s dynamic informer. The source keeps an in-memory map of `Feed` specs, signals `Changes()` on informer events, and exposes `ReportPoll` for status writeback. A new optional `OnPollComplete` hook on the scheduler (sibling of `OnPollOverrun`) surfaces per-feed poll outcomes; the `serve` command wires it to the source. Because polling is already lease-gated per feed via the coordinator, only the owning replica fires the hook, so status writes have no contention.

**Tech Stack:** Go 1.25, `k8s.io/client-go` (dynamic client + `dynamicinformer`), `k8s.io/apimachinery` (`schema.GroupVersionResource`, `unstructured`), `testcontainers-go/modules/k3s` for integration, existing Viper config + zerolog/OTEL.

**Full spec:** [GitHub issue #160](https://github.com/IAmBod/rss2msg/issues/160) is the standalone source of truth.

---

## Status / context

**Task 0 is already complete and committed** (`feat(feedsource): map Feed CR unstructured object to FeedSpec`, commit `6815657`). It added the dependency-free `specFromUnstructured` mapping plus `objString`/`objStringSlice`/`objStringMap` helpers and `TestKubernetesSpecFromUnstructured` in `internal/feedsource/kubernetes{,_test}.go`. Start at Task 1.

## File structure

| File | Responsibility |
| --- | --- |
| `internal/feedsource/kubernetes.go` | The `Kubernetes` source: GVR, options, constructors, informer, `Feeds`/`Changes`/`Name`/`Close`, `ReportPoll`. (Already holds `specFromUnstructured`.) |
| `internal/feedsource/kubernetes_test.go` | Unit tests against `client-go/dynamic/fake`. (Already holds the mapping test.) |
| `internal/feedsource/kubernetes_integration_test.go` | k3s testcontainers round-trip (`//go:build integration`). |
| `internal/config/config.go` | `KubernetesFeedSourceConfig` struct + field on `FeedSourceConfig`. |
| `internal/config/validate.go` | `case "kubernetes"` validation. |
| `cmd/rss2msg/sources.go` | `case "kubernetes"` in `buildSources`. |
| `internal/scheduler/serve.go` | Plumb `onComplete` through `runFeedLoop`/`runTick`/`tick`. |
| `internal/scheduler/dynamic.go` | `OnPollComplete` field + `startFeed` wiring. |
| `cmd/rss2msg/main.go` | `serve` wires `OnPollComplete` → source `ReportPoll`. |
| `deploy/crds/feeds.rss2msg.io.yaml` | Standalone CRD (non-Helm users). |
| `deploy/helm/rss2msg/templates/feeds-crd.yaml` | CRD as a values-gated Helm template. |
| `deploy/helm/rss2msg/templates/feeds-rbac.yaml` | ServiceAccount role binding for watch + status. |
| `deploy/helm/rss2msg/values.yaml` | `feedSource.kubernetes.*` values. |
| `docs/how-to/get-feeds-from-kubernetes.md` | New how-to page. |
| `docs/reference/configuration.md`, `docs/how-to/load-feeds-dynamically.md` | Updated with the new source. |

---

## Task 1: Add Kubernetes client dependencies

**Files:**
- Modify: `go.mod`, `go.sum` (via `go get` + `task tidy`)

- [ ] **Step 1: Add the dependencies**

Run (pin to the current stable minor; `v0.31.x` is compatible with Go 1.25):

```bash
go get k8s.io/client-go@v0.31.3 k8s.io/apimachinery@v0.31.3
```

- [ ] **Step 2: Tidy and verify the module graph builds**

Run: `task tidy && go build ./...`
Expected: no errors; `go.mod` now lists `k8s.io/client-go` and `k8s.io/apimachinery` as direct deps.

- [ ] **Step 3: Commit**

```bash
git add go.mod go.sum
git commit -m "build(deps): add k8s.io/client-go and apimachinery for the CRD feed source

Refs #160"
```

---

## Task 2: The `Kubernetes` source — construction + Feeds()

**Files:**
- Modify: `internal/feedsource/kubernetes.go`
- Test: `internal/feedsource/kubernetes_test.go`

- [ ] **Step 1: Write the failing test** (append to `kubernetes_test.go`)

```go
func unstructuredFeed(name string, spec map[string]any) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: feedObject(name, spec)}
}

func TestKubernetesSourceFeeds(t *testing.T) {
	scheme := runtime.NewScheme()
	gvrToKind := map[schema.GroupVersionResource]string{feedGVR: "FeedList"}
	client := fake.NewSimpleDynamicClientWithCustomListKinds(scheme, gvrToKind,
		unstructuredFeed("a", map[string]any{"url": "https://e/a", "interval": "5m"}),
		unstructuredFeed("b", map[string]any{"url": "https://e/b"}),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, err := newKubernetesWithClient(ctx, "k8s", client, KubernetesOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })

	feeds, err := s.Feeds(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(feeds) != 2 {
		t.Fatalf("want 2 feeds, got %d: %+v", len(feeds), feeds)
	}
	urls := map[string]bool{}
	for _, f := range feeds {
		urls[f.URL] = true
	}
	if !urls["https://e/a"] || !urls["https://e/b"] {
		t.Fatalf("missing feeds: %+v", feeds)
	}
}
```

Add imports to the test file: `"context"`, `k8s.io/apimachinery/pkg/apis/meta/v1/unstructured`, `k8s.io/apimachinery/pkg/runtime`, `k8s.io/apimachinery/pkg/runtime/schema`, `k8s.io/client-go/dynamic/fake`.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/feedsource/ -run TestKubernetesSourceFeeds`
Expected: build failure — `undefined: feedGVR`, `KubernetesOptions`, `newKubernetesWithClient`.

- [ ] **Step 3: Write minimal implementation** (add to `kubernetes.go`)

```go
// Add to the existing import block:
//   "context"
//   "sync"
//   "time"
//   metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
//   "k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
//   "k8s.io/apimachinery/pkg/runtime/schema"
//   "k8s.io/client-go/dynamic"
//   "k8s.io/client-go/dynamic/dynamicinformer"
//   "k8s.io/client-go/tools/cache"
//   "github.com/iambod/rss2msg/internal/config"

var _ Source = (*Kubernetes)(nil)

// feedGVR is the GroupVersionResource for the Feed custom resource.
var feedGVR = schema.GroupVersionResource{Group: "rss2msg.io", Version: "v1", Resource: "feeds"}

const defaultResyncInterval = 10 * time.Minute

// KubernetesOptions configures a Kubernetes-backed feed source. The source
// watches Feed custom resources; it never creates or deletes them.
type KubernetesOptions struct {
	Name           string
	Namespace      string        // "" = all namespaces (cluster-wide watch)
	LabelSelector  string        // optional; scope which Feeds this instance owns
	ResyncInterval time.Duration // default 10m
	WriteStatus    bool          // enable .status writeback
}

// Kubernetes is a feed source backed by Feed custom resources, watched via a
// dynamic informer. Feeds() serves the informer's current cache; Changes()
// fires on add/update/delete.
type Kubernetes struct {
	name        string
	client      dynamic.Interface
	namespace   string
	writeStatus bool

	factory  dynamicinformer.DynamicSharedInformerFactory
	informer cache.SharedIndexInformer
	stop     chan struct{}
	closeErr sync.Once

	changes chan struct{}

	mu    sync.RWMutex
	specs map[string]FeedSpec        // key: namespace/name
	index map[string]types.Namespaced // key: feed URL -> {namespace, name}
}

// newKubernetesWithClient builds a source from an injected dynamic client (used
// by both NewKubernetes and tests). It starts the informer and blocks until the
// initial cache sync completes.
func newKubernetesWithClient(ctx context.Context, name string, client dynamic.Interface, opts KubernetesOptions) (*Kubernetes, error) {
	resync := opts.ResyncInterval
	if resync <= 0 {
		resync = defaultResyncInterval
	}
	ns := opts.Namespace
	tweak := func(lo *metav1.ListOptions) {
		if opts.LabelSelector != "" {
			lo.LabelSelector = opts.LabelSelector
		}
	}
	factory := dynamicinformer.NewFilteredDynamicSharedInformerFactory(client, resync, ns, tweak)
	informer := factory.ForResource(feedGVR).Informer()

	k := &Kubernetes{
		name:        name,
		client:      client,
		namespace:   ns,
		writeStatus: opts.WriteStatus,
		factory:     factory,
		informer:    informer,
		stop:        make(chan struct{}),
		changes:     make(chan struct{}, 1),
		specs:       map[string]FeedSpec{},
		index:       map[string]types.Namespaced{},
	}

	_, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { k.upsert(obj); k.signal() },
		UpdateFunc: func(_, obj any) { k.upsert(obj); k.signal() },
		DeleteFunc: func(obj any) { k.remove(obj); k.signal() },
	})
	if err != nil {
		return nil, fmt.Errorf("kubernetes feed source %q: add handler: %w", name, err)
	}

	factory.Start(k.stop)
	if !cache.WaitForCacheSync(ctx.Done(), informer.HasSynced) {
		k.Close()
		return nil, fmt.Errorf("kubernetes feed source %q: cache sync failed", name)
	}
	return k, nil
}

func (k *Kubernetes) Name() string { return k.name }

func (k *Kubernetes) Changes() <-chan struct{} { return k.changes }

func (k *Kubernetes) Feeds(_ context.Context) ([]config.FeedConfig, error) {
	k.mu.RLock()
	specs := make([]FeedSpec, 0, len(k.specs))
	for _, s := range k.specs {
		specs = append(specs, s)
	}
	k.mu.RUnlock()
	return SpecsToConfigs(specs)
}

// Close stops the informer. Safe to call more than once.
func (k *Kubernetes) Close() error {
	k.closeErr.Do(func() { close(k.stop) })
	return nil
}

// signal does a non-blocking send so a burst of events coalesces into one wake.
func (k *Kubernetes) signal() {
	select {
	case k.changes <- struct{}{}:
	default:
	}
}

func (k *Kubernetes) upsert(obj any) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return
	}
	spec, err := specFromUnstructured(u.Object)
	if err != nil {
		return // invalid CR (e.g. missing url): skip; apiserver schema should prevent this
	}
	key := u.GetNamespace() + "/" + u.GetName()
	k.mu.Lock()
	k.specs[key] = spec
	k.index[spec.URL] = types.Namespaced{Namespace: u.GetNamespace(), Name: u.GetName()}
	k.mu.Unlock()
}

func (k *Kubernetes) remove(obj any) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		if tomb, ok := obj.(cache.DeletedFinalStateUnknown); ok {
			u, ok = tomb.Obj.(*unstructured.Unstructured)
			if !ok {
				return
			}
		} else {
			return
		}
	}
	key := u.GetNamespace() + "/" + u.GetName()
	k.mu.Lock()
	if s, ok := k.specs[key]; ok {
		delete(k.index, s.URL)
	}
	delete(k.specs, key)
	k.mu.Unlock()
}
```

Add a small helper type in `kubernetes.go` (or reuse an existing one if present):

```go
// (define near the top of kubernetes.go, below feedGVR)
//
// Replace the `types.Namespaced` references above with this local type to avoid
// importing k8s types for a 2-field struct:
//
//   type namespacedName struct{ Namespace, Name string }
//
// and change the index map to map[string]namespacedName.
```

> **Note for the implementer:** Use the local `namespacedName` struct (not `types.Namespaced`, which does not exist in apimachinery). Update the struct field, map type, and `upsert` accordingly. The plan names it `types.Namespaced` only as a placeholder to flag "a {namespace,name} pair"; the real code uses `namespacedName`.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/feedsource/ -run TestKubernetesSourceFeeds`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/feedsource/kubernetes.go internal/feedsource/kubernetes_test.go
git commit -m "feat(feedsource): kubernetes source watches Feed CRs via dynamic informer

Refs #160"
```

---

## Task 3: `Changes()` fires on informer events

**Files:**
- Test: `internal/feedsource/kubernetes_test.go`

- [ ] **Step 1: Write the failing test**

```go
func TestKubernetesSourceSignalsOnAdd(t *testing.T) {
	scheme := runtime.NewScheme()
	gvrToKind := map[schema.GroupVersionResource]string{feedGVR: "FeedList"}
	client := fake.NewSimpleDynamicClientWithCustomListKinds(scheme, gvrToKind)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, err := newKubernetesWithClient(ctx, "k8s", client, KubernetesOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })

	// drain the initial-sync signal if any
	select {
	case <-s.Changes():
	case <-time.After(100 * time.Millisecond):
	}

	_, err = client.Resource(feedGVR).Namespace("feeds").Create(ctx,
		unstructuredFeed("c", map[string]any{"url": "https://e/c"}), metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}

	select {
	case <-s.Changes():
	case <-time.After(2 * time.Second):
		t.Fatal("expected a change signal after Create")
	}
}
```

Add test imports: `metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"`.

- [ ] **Step 2: Run test to verify it fails (or passes for the right reason)**

Run: `go test ./internal/feedsource/ -run TestKubernetesSourceSignalsOnAdd`
Expected: PASS already (handlers wired in Task 2). If it fails, the event handler or `signal()` is wrong — fix before continuing. This test locks the change-signal contract against regressions.

- [ ] **Step 3: Commit**

```bash
git add internal/feedsource/kubernetes_test.go
git commit -m "test(feedsource): kubernetes source signals Changes on CR add

Refs #160"
```

---

## Task 4: Production constructor `NewKubernetes` (rest.Config)

**Files:**
- Modify: `internal/feedsource/kubernetes.go`
- Test: `internal/feedsource/kubernetes_test.go`

- [ ] **Step 1: Write the failing test** (kubeconfig error path is unit-testable; in-cluster is covered by the k3s integration test)

```go
func TestNewKubernetesBadKubeconfig(t *testing.T) {
	ctx := context.Background()
	_, err := NewKubernetes(ctx, KubernetesOptions{Name: "k8s"}, "/nonexistent/kubeconfig")
	if err == nil {
		t.Fatal("expected an error for a missing kubeconfig path")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/feedsource/ -run TestNewKubernetesBadKubeconfig`
Expected: build failure — `undefined: NewKubernetes`.

- [ ] **Step 3: Write minimal implementation** (add to `kubernetes.go`)

```go
// Add imports: "k8s.io/client-go/rest", "k8s.io/client-go/tools/clientcmd".

// NewKubernetes builds a source from cluster credentials. When kubeconfig is
// empty it uses in-cluster config (the pod's ServiceAccount); otherwise it loads
// the named kubeconfig file (for local/out-of-cluster use).
func NewKubernetes(ctx context.Context, opts KubernetesOptions, kubeconfig string) (*Kubernetes, error) {
	var (
		cfg *rest.Config
		err error
	)
	if kubeconfig == "" {
		cfg, err = rest.InClusterConfig()
	} else {
		cfg, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
	}
	if err != nil {
		return nil, fmt.Errorf("kubernetes feed source %q: rest config: %w", opts.Name, err)
	}
	client, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("kubernetes feed source %q: dynamic client: %w", opts.Name, err)
	}
	return newKubernetesWithClient(ctx, opts.Name, client, opts)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/feedsource/ -run TestNewKubernetesBadKubeconfig`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/feedsource/kubernetes.go internal/feedsource/kubernetes_test.go
git commit -m "feat(feedsource): NewKubernetes builds source from in-cluster/kubeconfig creds

Refs #160"
```

---

## Task 5: Config struct + validation

**Files:**
- Modify: `internal/config/config.go:595-601`, `internal/config/validate.go:828-854`
- Test: `internal/config/validate_test.go` (add cases)

- [ ] **Step 1: Write the failing test** (append to the existing validate test file; match its table style)

```go
func TestValidateKubernetesFeedSource(t *testing.T) {
	base := minimalValidConfig(t) // helper already used by sibling tests; if absent, build a Config with one sink "default" and one valid feed-source entry
	base.FeedSources = []config.FeedSourceConfig{{Type: "kubernetes"}}
	if _, err := base.Validate(); err != nil {
		t.Fatalf("kubernetes source with defaults should validate: %v", err)
	}

	base.FeedSources = []config.FeedSourceConfig{{
		Type:       "kubernetes",
		Kubernetes: config.KubernetesFeedSourceConfig{LabelSelector: "bad=="},
	}}
	if _, err := base.Validate(); err == nil {
		t.Fatal("an invalid labelSelector should fail validation")
	}
}
```

> If `minimalValidConfig` / `base.Validate()` differ from the file's existing helpers, mirror whatever pattern the neighbouring `TestValidate*` functions use (they construct a `config.Config` and call the package validation entry point). Keep the two assertions: defaults pass; bad label selector fails.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/config/ -run TestValidateKubernetesFeedSource`
Expected: build failure — `undefined: config.KubernetesFeedSourceConfig`.

- [ ] **Step 3: Implement the struct** (`config.go`, in the `FeedSourceConfig` block)

```go
type FeedSourceConfig struct {
	Type       string                     `mapstructure:"type"`
	Name       string                     `mapstructure:"name"`
	Path       string                     `mapstructure:"path"`
	Interval   time.Duration              `mapstructure:"interval"`
	Postgres   PostgresFeedSourceConfig   `mapstructure:"postgres"`
	Kubernetes KubernetesFeedSourceConfig `mapstructure:"kubernetes"`
}

// KubernetesFeedSourceConfig configures a Kubernetes-backed feed source. The
// desired feed list is read from Feed custom resources (rss2msg.io/v1) watched
// via a dynamic informer. The source never creates or deletes Feeds.
type KubernetesFeedSourceConfig struct {
	Namespace      string        `mapstructure:"namespace"`       // "" = all namespaces (cluster-wide watch)
	Kubeconfig     string        `mapstructure:"kubeconfig"`      // "" = in-cluster config
	LabelSelector  string        `mapstructure:"label_selector"`  // optional
	ResyncInterval time.Duration `mapstructure:"resync_interval"` // default 10m
	WriteStatus    *bool         `mapstructure:"write_status"`    // default true; pointer so "unset" != "false"
}
```

- [ ] **Step 4: Implement validation** (`validate.go`, add a case before `case "":`)

```go
		case "kubernetes":
			if s.Kubernetes.LabelSelector != "" {
				if _, err := labels.Parse(s.Kubernetes.LabelSelector); err != nil {
					return *warnings, fmt.Errorf("feed_sources[%d] (kubernetes): invalid label_selector: %w", i, err)
				}
			}
			if s.Kubernetes.ResyncInterval != 0 && s.Kubernetes.ResyncInterval < time.Second {
				return *warnings, fmt.Errorf("feed_sources[%d] (kubernetes): resync_interval %v is below the 1s minimum", i, s.Kubernetes.ResyncInterval)
			}
```

Add the import `"k8s.io/apimachinery/pkg/labels"` to `validate.go`.

- [ ] **Step 5: Run test to verify it passes**

Run: `go test ./internal/config/ -run TestValidateKubernetesFeedSource`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/config/config.go internal/config/validate.go internal/config/validate_test.go
git commit -m "feat(config): kubernetes feed-source config + validation

Refs #160"
```

---

## Task 6: Wire `buildSources`

**Files:**
- Modify: `cmd/rss2msg/sources.go:58-96`
- Test: `cmd/rss2msg/sources_test.go`

- [ ] **Step 1: Write the failing test** (mirror the existing `buildSources` table test; the kubernetes case will fail to connect in-cluster, so assert the *error path* is the connect attempt, not an "unsupported type" error)

```go
func TestBuildSourcesKubernetesUnsupportedTypeGone(t *testing.T) {
	cfg := config.Config{FeedSources: []config.FeedSourceConfig{{Type: "kubernetes"}}}
	_, cleanup, err := buildSources(cfg)
	if cleanup != nil {
		cleanup()
	}
	// Outside a cluster with no kubeconfig, NewKubernetes fails at rest.InClusterConfig.
	// The point of this test: the error must NOT be "unsupported type".
	if err == nil {
		t.Skip("running inside a cluster; constructor succeeded")
	}
	if strings.Contains(err.Error(), "unsupported type") {
		t.Fatalf("kubernetes should be a recognised source type, got: %v", err)
	}
}
```

Add imports `"strings"` and `"github.com/iambod/rss2msg/internal/config"` if not present.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./cmd/rss2msg/ -run TestBuildSourcesKubernetesUnsupportedTypeGone`
Expected: FAIL — error is currently `unsupported type "kubernetes"`.

- [ ] **Step 3: Implement the case** (`sources.go`, before `default:`)

```go
		case "kubernetes":
			writeStatus := true
			if sc.Kubernetes.WriteStatus != nil {
				writeStatus = *sc.Kubernetes.WriteStatus
			}
			k, err := feedsource.NewKubernetes(context.Background(), feedsource.KubernetesOptions{
				Name:           name,
				Namespace:      sc.Kubernetes.Namespace,
				LabelSelector:  sc.Kubernetes.LabelSelector,
				ResyncInterval: sc.Kubernetes.ResyncInterval,
				WriteStatus:    writeStatus,
			}, sc.Kubernetes.Kubeconfig)
			if err != nil {
				closeAll(closers)
				return nil, nil, err
			}
			closers = append(closers, func() { _ = k.Close() })
			sources = append(sources, k)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./cmd/rss2msg/ -run TestBuildSourcesKubernetesUnsupportedTypeGone`
Expected: PASS (or SKIP inside a cluster).

- [ ] **Step 5: Commit**

```bash
git add cmd/rss2msg/sources.go cmd/rss2msg/sources_test.go
git commit -m "feat(cmd): wire kubernetes feed source into buildSources

Refs #160"
```

---

## Task 7: `OnPollComplete` hook — plumb through the scheduler loop

**Files:**
- Modify: `internal/scheduler/serve.go:84-117`
- Test: `internal/scheduler/serve_test.go` (or `dynamic_test.go`)

- [ ] **Step 1: Write the failing test** (use a fake pipeline that returns a fixed number of changes)

```go
func TestServeFiresOnPollComplete(t *testing.T) {
	p := &fakePipeline{url: "https://e/x", changes: 3} // see note below
	got := make(chan int, 1)
	cfg := ServeConfig{
		Pipelines:    []FeedPipeline{p},
		Intervals:    map[string]time.Duration{"https://e/x": time.Hour},
		DrainTimeout: time.Second,
		OnPollComplete: func(feedURL string, changeCount int, err error, when time.Time) {
			select {
			case got <- changeCount:
			default:
			}
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = Serve(ctx, cfg) }()
	select {
	case n := <-got:
		if n != 3 {
			t.Fatalf("changeCount = %d, want 3", n)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("OnPollComplete never fired")
	}
	cancel()
}
```

> `fakePipeline` must implement `FeedURL() string` and `RunOnce(...) ([]model.Change, error)` returning a slice of length `changes`. If the test file already has a fake pipeline type, extend it; otherwise add one returning `make([]model.Change, p.changes)`.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/scheduler/ -run TestServeFiresOnPollComplete`
Expected: build failure — `ServeConfig` has no `OnPollComplete` field.

- [ ] **Step 3: Implement the plumbing**

Add the field to `ServeConfig`:

```go
	// OnPollComplete, if set, is called after every poll with the number of
	// changes produced and the poll error (nil on success). Optional.
	OnPollComplete func(feedURL string, changeCount int, err error, when time.Time)
```

Thread an `onComplete func(changeCount int, err error, when time.Time)` through `runFeedLoop` → `runTick` → `tick`. Updated signatures and bodies:

```go
func runFeedLoop(ctx context.Context, p FeedPipeline, interval time.Duration, collect func(error), onOverrun func(took time.Duration), onComplete func(int, error, time.Time)) {
	t := time.NewTicker(interval)
	defer t.Stop()
	runTick(ctx, p, interval, collect, onOverrun, onComplete)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			runTick(ctx, p, interval, collect, onOverrun, onComplete)
		}
	}
}

func runTick(ctx context.Context, p FeedPipeline, interval time.Duration, collect func(error), onOverrun func(took time.Duration), onComplete func(int, error, time.Time)) {
	start := time.Now()
	tick(ctx, p, collect, onComplete)
	took := time.Since(start)
	if onOverrun != nil && took > interval && ctx.Err() == nil {
		onOverrun(took)
	}
}

func tick(ctx context.Context, p FeedPipeline, collect func(error), onComplete func(int, error, time.Time)) {
	when := time.Now().UTC()
	changes, err := p.RunOnce(ctx, p.FeedURL(), when)
	if err != nil && !errors.Is(err, context.Canceled) {
		collect(err)
	}
	if onComplete != nil && ctx.Err() == nil {
		onComplete(len(changes), err, when)
	}
}
```

In `Serve`, build and pass the per-feed `onComplete` closure (alongside the existing `onOverrun`):

```go
		var onComplete func(int, error, time.Time)
		if cfg.OnPollComplete != nil {
			url := p.FeedURL()
			onComplete = func(n int, err error, when time.Time) { cfg.OnPollComplete(url, n, err, when) }
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			runFeedLoop(ctx, p, interval, collect, onOverrun, onComplete)
		}()
```

Update `dynamic.go`'s `startFeed` call to `runFeedLoop(fctx, p, fc.Interval, func(error) {}, onOverrun, onComplete)` — see Task 8 for the `onComplete` it passes.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/scheduler/ -run TestServeFiresOnPollComplete`
Expected: PASS.

- [ ] **Step 5: Run the full scheduler suite** (the signature change touches dynamic.go)

Run: `go test ./internal/scheduler/...`
Expected: PASS (after Task 8 compiles `dynamic.go`; if you implement Task 7 and 8 in one pass the package builds — otherwise temporarily pass `nil` for `onComplete` in `startFeed`).

- [ ] **Step 6: Commit**

```bash
git add internal/scheduler/serve.go internal/scheduler/serve_test.go
git commit -m "feat(scheduler): OnPollComplete hook surfacing per-feed poll outcomes

Refs #160"
```

---

## Task 8: `OnPollComplete` on `DynamicConfig` + startFeed wiring

**Files:**
- Modify: `internal/scheduler/dynamic.go:22-37, 109, 128-143`
- Test: `internal/scheduler/dynamic_test.go`

- [ ] **Step 1: Write the failing test** (drive ServeDynamic with a one-feed provider; assert the hook fires)

```go
func TestServeDynamicFiresOnPollComplete(t *testing.T) {
	fc := config.FeedConfig{URL: "https://e/x", Interval: time.Hour}
	prov := &staticProvider{feeds: []config.FeedConfig{fc}} // existing test helper or a minimal FeedProvider
	got := make(chan int, 1)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		_ = ServeDynamic(ctx, DynamicConfig{
			Provider:     prov,
			Factory:      func(config.FeedConfig) (FeedPipeline, error) { return &fakePipeline{url: "https://e/x", changes: 2}, nil },
			DrainTimeout: time.Second,
			OnPollComplete: func(url string, n int, err error, when time.Time) {
				select {
				case got <- n:
				default:
				}
			},
		})
	}()
	select {
	case n := <-got:
		if n != 2 {
			t.Fatalf("changeCount = %d, want 2", n)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("OnPollComplete never fired under ServeDynamic")
	}
	cancel()
}
```

> Reuse the existing dynamic-test `FeedProvider` fake if present; otherwise add a minimal `staticProvider` with `Desired()` returning the feeds and a never-firing `Changes()`.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/scheduler/ -run TestServeDynamicFiresOnPollComplete`
Expected: build failure — `DynamicConfig` has no `OnPollComplete`.

- [ ] **Step 3: Implement**

Add to `DynamicConfig`:

```go
	// OnPollComplete, if set, is called after every poll of a running feed with
	// the change count and poll error. Optional.
	OnPollComplete func(feedURL string, changeCount int, err error, when time.Time)
```

Change `startFeed`'s signature and body to accept and bind the callback, and update the call site at line 109:

```go
// call site:
running[url] = startFeed(ctx, byURL[url], p, cfg.OnPollOverrun, cfg.OnPollComplete)

// startFeed:
func startFeed(parent context.Context, fc config.FeedConfig, p FeedPipeline,
	onPollOverrun func(feedURL string, took, interval time.Duration),
	onPollComplete func(feedURL string, changeCount int, err error, when time.Time),
) *runningFeed {
	fctx, cancel := context.WithCancel(parent)
	done := make(chan struct{})
	var onOverrun func(took time.Duration)
	if onPollOverrun != nil {
		onOverrun = func(took time.Duration) { onPollOverrun(fc.URL, took, fc.Interval) }
	}
	var onComplete func(int, error, time.Time)
	if onPollComplete != nil {
		onComplete = func(n int, err error, when time.Time) { onPollComplete(fc.URL, n, err, when) }
	}
	go func() {
		defer close(done)
		runFeedLoop(fctx, p, fc.Interval, func(error) {}, onOverrun, onComplete)
	}()
	return &runningFeed{cfg: fc, cancel: cancel, done: done}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/scheduler/...`
Expected: PASS (whole package builds and is green).

- [ ] **Step 5: Commit**

```bash
git add internal/scheduler/dynamic.go internal/scheduler/dynamic_test.go
git commit -m "feat(scheduler): OnPollComplete on DynamicConfig wired through startFeed

Refs #160"
```

---

## Task 9: Status writeback — `ReportPoll`

**Files:**
- Modify: `internal/feedsource/kubernetes.go`
- Test: `internal/feedsource/kubernetes_test.go`

- [ ] **Step 1: Write the failing test**

```go
func TestKubernetesReportPollWritesStatus(t *testing.T) {
	scheme := runtime.NewScheme()
	gvrToKind := map[schema.GroupVersionResource]string{feedGVR: "FeedList"}
	client := fake.NewSimpleDynamicClientWithCustomListKinds(scheme, gvrToKind,
		unstructuredFeed("a", map[string]any{"url": "https://e/a"}),
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, err := newKubernetesWithClient(ctx, "k8s", client, KubernetesOptions{WriteStatus: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })

	when := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)
	s.ReportPoll(ctx, "https://e/a", 2, nil, when)

	got, err := client.Resource(feedGVR).Namespace("feeds").Get(ctx, "a", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	count, found, _ := unstructured.NestedInt64(got.Object, "status", "lastChangeCount")
	if !found || count != 2 {
		t.Fatalf("status.lastChangeCount = %d found=%v", count, found)
	}
	ts, _, _ := unstructured.NestedString(got.Object, "status", "lastPollTime")
	if ts != "2026-06-15T12:00:00Z" {
		t.Fatalf("status.lastPollTime = %q", ts)
	}
}

func TestKubernetesReportPollDisabled(t *testing.T) {
	scheme := runtime.NewScheme()
	gvrToKind := map[schema.GroupVersionResource]string{feedGVR: "FeedList"}
	client := fake.NewSimpleDynamicClientWithCustomListKinds(scheme, gvrToKind,
		unstructuredFeed("a", map[string]any{"url": "https://e/a"}),
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, err := newKubernetesWithClient(ctx, "k8s", client, KubernetesOptions{WriteStatus: false})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })

	s.ReportPoll(ctx, "https://e/a", 2, nil, time.Now().UTC())
	got, _ := client.Resource(feedGVR).Namespace("feeds").Get(ctx, "a", metav1.GetOptions{})
	if _, found, _ := unstructured.NestedMap(got.Object, "status"); found {
		t.Fatal("writeStatus=false must not write .status")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/feedsource/ -run TestKubernetesReportPoll`
Expected: build failure — `s.ReportPoll undefined`.

- [ ] **Step 3: Implement** (add to `kubernetes.go`)

```go
// ReportPoll writes poll outcome to the Feed CR's status subresource. It is a
// no-op when writeStatus is false or when feedURL is not owned by this source
// (e.g. it came from another source). Errors are swallowed (best-effort, logged
// by the caller is unnecessary): status is observability, never on the hot path.
func (k *Kubernetes) ReportPoll(ctx context.Context, feedURL string, changeCount int, pollErr error, when time.Time) {
	if !k.writeStatus {
		return
	}
	k.mu.RLock()
	nn, ok := k.index[feedURL]
	k.mu.RUnlock()
	if !ok {
		return
	}

	cur, err := k.client.Resource(feedGVR).Namespace(nn.Namespace).Get(ctx, nn.Name, metav1.GetOptions{})
	if err != nil {
		return
	}
	status := map[string]any{
		"observedGeneration": cur.GetGeneration(),
		"lastPollTime":       when.UTC().Format(time.RFC3339),
		"lastChangeCount":    int64(changeCount),
	}
	readyStatus, reason := "True", "Polled"
	if pollErr != nil {
		status["lastError"] = pollErr.Error()
		readyStatus, reason = "False", "PollError"
	} else {
		status["lastError"] = ""
	}
	status["conditions"] = []any{map[string]any{
		"type":               "Ready",
		"status":             readyStatus,
		"reason":             reason,
		"lastTransitionTime": when.UTC().Format(time.RFC3339),
	}}
	if err := unstructured.SetNestedField(cur.Object, status, "status"); err != nil {
		return
	}
	_, _ = k.client.Resource(feedGVR).Namespace(nn.Namespace).UpdateStatus(ctx, cur, metav1.UpdateOptions{})
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/feedsource/ -run TestKubernetesReportPoll`
Expected: PASS (both cases).

- [ ] **Step 5: Commit**

```bash
git add internal/feedsource/kubernetes.go internal/feedsource/kubernetes_test.go
git commit -m "feat(feedsource): write poll status back to Feed CR .status subresource

Refs #160"
```

---

## Task 10: Wire status writeback in `serve`

**Files:**
- Modify: `cmd/rss2msg/main.go:54-121`
- Test: covered by the existing `serve_test.go` smoke test (the wiring must compile and not change default behaviour); the behaviour itself is covered by the k3s integration test (Task 13).

- [ ] **Step 1: Implement the wiring** (after `sources, closeSources, err := buildSources(cfg)` and before `ServeDynamic`)

```go
			// Collect kubernetes sources for status writeback (best-effort).
			var k8sSources []*feedsource.Kubernetes
			for _, src := range sources {
				if ks, ok := src.(*feedsource.Kubernetes); ok {
					k8sSources = append(k8sSources, ks)
				}
			}
```

Add the hook to the `DynamicConfig` literal (after `OnPollOverrun`):

```go
				OnPollComplete: func(feedURL string, changeCount int, err error, when time.Time) {
					for _, ks := range k8sSources {
						ks.ReportPoll(ctx, feedURL, changeCount, err, when)
					}
				},
```

When `k8sSources` is empty the closure is a cheap no-op, so setting it unconditionally is fine.

- [ ] **Step 2: Build and run the serve smoke test**

Run: `go test ./cmd/rss2msg/ -run TestServe`
Expected: PASS (wiring compiles; existing behaviour unchanged).

- [ ] **Step 3: Commit**

```bash
git add cmd/rss2msg/main.go
git commit -m "feat(cmd): serve writes Feed CR status via OnPollComplete

Refs #160"
```

---

## Task 11: Manifests — CRD + RBAC + Helm values

**Files:**
- Create: `deploy/crds/feeds.rss2msg.io.yaml`
- Create: `deploy/helm/rss2msg/templates/feeds-crd.yaml`
- Create: `deploy/helm/rss2msg/templates/feeds-rbac.yaml`
- Modify: `deploy/helm/rss2msg/values.yaml`

- [ ] **Step 1: Write the CRD** (`deploy/crds/feeds.rss2msg.io.yaml`)

```yaml
apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: feeds.rss2msg.io
spec:
  group: rss2msg.io
  scope: Namespaced
  names:
    kind: Feed
    listKind: FeedList
    plural: feeds
    singular: feed
  versions:
    - name: v1
      served: true
      storage: true
      subresources:
        status: {}
      additionalPrinterColumns:
        - name: URL
          type: string
          jsonPath: .spec.url
        - name: Last Poll
          type: date
          jsonPath: .status.lastPollTime
        - name: Changes
          type: integer
          jsonPath: .status.lastChangeCount
      schema:
        openAPIV3Schema:
          type: object
          properties:
            spec:
              type: object
              required: [url]
              properties:
                url:
                  type: string
                interval:
                  type: string
                  pattern: '^[0-9]+(ns|us|µs|ms|s|m|h)([0-9]+(ns|us|µs|ms|s|m|h))*$'
                sinks:
                  type: array
                  items:
                    type: string
                http:
                  type: object
                  properties:
                    timeout:
                      type: string
                    headers:
                      type: object
                      additionalProperties:
                        type: string
            status:
              type: object
              x-kubernetes-preserve-unknown-fields: true
```

- [ ] **Step 2: Validate the CRD YAML parses**

Run: `go run sigs.k8s.io/yaml@latest 2>/dev/null || python3 -c "import yaml,sys; yaml.safe_load(open('deploy/crds/feeds.rss2msg.io.yaml'))" && echo OK`
Expected: `OK` (YAML is well-formed). If `kubectl` is available: `kubectl apply --dry-run=client -f deploy/crds/feeds.rss2msg.io.yaml`.

- [ ] **Step 3: Add the Helm CRD template** (`templates/feeds-crd.yaml`) — same content as Step 1, wrapped:

```yaml
{{- if .Values.feedSource.kubernetes.crd.install }}
{{ .Files.Get "crds/feeds.rss2msg.io.yaml" | nindent 0 }}
{{- end }}
```

> Copy `deploy/crds/feeds.rss2msg.io.yaml` to `deploy/helm/rss2msg/crds/feeds.rss2msg.io.yaml` so `.Files.Get` can read it (Helm packages files under the chart dir). Alternatively inline the CRD body directly in the template guarded by the `if`. Pick one; the inline form avoids the file copy.

- [ ] **Step 4: Add the RBAC template** (`templates/feeds-rbac.yaml`)

```yaml
{{- if .Values.feedSource.kubernetes.rbac.create }}
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: {{ include "rss2msg.fullname" . }}-feeds
  labels:
    {{- include "rss2msg.labels" . | nindent 4 }}
rules:
  - apiGroups: ["rss2msg.io"]
    resources: ["feeds"]
    verbs: ["get", "list", "watch"]
{{- if .Values.feedSource.kubernetes.writeStatus }}
  - apiGroups: ["rss2msg.io"]
    resources: ["feeds/status"]
    verbs: ["get", "update", "patch"]
{{- end }}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: {{ include "rss2msg.fullname" . }}-feeds
  labels:
    {{- include "rss2msg.labels" . | nindent 4 }}
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: {{ include "rss2msg.fullname" . }}-feeds
subjects:
  - kind: ServiceAccount
    name: {{ include "rss2msg.serviceAccountName" . }}
    namespace: {{ .Release.Namespace }}
{{- end }}
```

> Confirm the helper names (`rss2msg.fullname`, `rss2msg.labels`, `rss2msg.serviceAccountName`) against `deploy/helm/rss2msg/templates/_helpers.tpl`; use whatever the existing `serviceaccount.yaml` uses.

- [ ] **Step 5: Add values** (`values.yaml`, new top-level block)

```yaml
feedSource:
  kubernetes:
    crd:
      install: true
    rbac:
      create: true
    writeStatus: true
```

- [ ] **Step 6: Lint the chart** (if `helm` is installed)

Run: `helm lint deploy/helm/rss2msg && helm template deploy/helm/rss2msg --set feedSource.kubernetes.crd.install=true >/dev/null && echo OK`
Expected: `OK`. If `helm` is unavailable, note it in the PR and rely on the integration test applying the standalone CRD.

- [ ] **Step 7: Commit**

```bash
git add deploy/crds/feeds.rss2msg.io.yaml deploy/helm/rss2msg/templates/feeds-crd.yaml deploy/helm/rss2msg/templates/feeds-rbac.yaml deploy/helm/rss2msg/values.yaml
git commit -m "feat(deploy): Feed CRD, RBAC, and Helm wiring for the kubernetes source

Refs #160"
```

---

## Task 12: Documentation

**Files:**
- Create: `docs/how-to/get-feeds-from-kubernetes.md`
- Modify: `docs/reference/configuration.md`, `docs/how-to/load-feeds-dynamically.md`

- [ ] **Step 1: Write the how-to page** with the standard frontmatter (`title`, `type`, `tags`, `summary`, `updated`) and a `## Related` footer, matching sibling pages in `docs/how-to/`. Ground every statement in the config keys from Task 5 and the manifests from Task 11. Cover: applying the CRD, RBAC, a `feed_sources: [{type: kubernetes}]` config snippet, the `Feed` example, and `kubectl get feeds` showing status.

- [ ] **Step 2: Cross-link** from `docs/how-to/load-feeds-dynamically.md` (add the kubernetes source alongside file/postgres) and document the `kubernetes` source keys in `docs/reference/configuration.md` next to the existing `feed_sources` entries.

- [ ] **Step 3: Run the docs link checker**

Run: `bash scripts/check-doc-links.sh`
Expected: `OK: all relative doc links resolve`.

- [ ] **Step 4: Commit**

```bash
git add docs/how-to/get-feeds-from-kubernetes.md docs/how-to/load-feeds-dynamically.md docs/reference/configuration.md
git commit -m "docs: document the kubernetes CRD feed source

Refs #160"
```

---

## Task 13: k3s integration test

**Files:**
- Create: `internal/feedsource/kubernetes_integration_test.go` (`//go:build integration`)
- Modify: `go.mod` (adds `testcontainers-go/modules/k3s`)

- [ ] **Step 1: Write the integration test**

```go
//go:build integration

package feedsource_test

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/clientcmd"
	"github.com/testcontainers/testcontainers-go/modules/k3s"

	"github.com/iambod/rss2msg/internal/feedsource"
)

func TestKubernetesSourceK3sRoundTrip(t *testing.T) {
	ctx := context.Background()
	k3sC, err := k3s.Run(ctx, "rancher/k3s:v1.31.2-k3s1")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = k3sC.Terminate(ctx) })

	kubeYAML, err := k3sC.GetKubeConfig(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// write kubeYAML to a temp file, apply the CRD + a Feed via the dynamic client,
	// then construct feedsource.NewKubernetes with that kubeconfig path and assert
	// Feeds() returns the feed and ReportPoll writes .status. (Full body: build a
	// dynamic client from clientcmd.RESTConfigFromKubeConfig(kubeYAML), apply
	// deploy/crds/feeds.rss2msg.io.yaml, create a Feed, wait for Established.)
	_ = kubeYAML
	_ = feedGVRForTest()
}

func feedGVRForTest() schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: "rss2msg.io", Version: "v1", Resource: "feeds"}
}
```

> Flesh out the body: load the CRD from `../../deploy/crds/feeds.rss2msg.io.yaml`, apply it with the dynamic client, poll until the CRD is `Established`, create a `Feed`, then run `feedsource.NewKubernetes` against the kubeconfig and assert `Feeds()` + `ReportPoll` round-trip against the real apiserver. Mirror the structure of `postgres_integration_test.go` (testcontainers lifecycle, `t.Cleanup`).

- [ ] **Step 2: Tidy (new testcontainers module) and run**

Run: `task tidy && task test-integration` (needs Docker)
Expected: the new test passes; document the run in the PR. If Docker is unavailable in your environment, say so explicitly and ensure the unit tests (fake client) cover the logic.

- [ ] **Step 3: Commit**

```bash
git add internal/feedsource/kubernetes_integration_test.go go.mod go.sum
git commit -m "test(feedsource): k3s integration round-trip for the kubernetes source

Refs #160"
```

---

## Task 14: Final gates + PR

- [ ] **Step 1: Full unit suite, vet, lint, tidy**

Run:
```bash
task test && task vet && task lint && task tidy && git diff --quiet go.mod go.sum && echo "TIDY CLEAN"
```
Expected: all green; `TIDY CLEAN` printed (no uncommitted go.mod/go.sum drift).

- [ ] **Step 2: Confirm only intended files are staged across the branch**

Run: `git log --oneline main..HEAD` and `git diff --stat main..HEAD`
Expected: only the files listed in this plan's File structure; no Obsidian/vault noise.

- [ ] **Step 3: Push and open the PR** (link issue #160; note in the body whether `task test-integration` was run, and the new `client-go` dependency)

```bash
git push -u origin feat/k8s-crd-feed-source
gh pr create --repo IAmBod/rss2msg --base main --title "feat: Kubernetes CRD feed source (#160)" --body "<summary + closes #160 + integration-test note + deps note>"
```

---

## Self-review notes

- **Spec coverage:** CRD (Task 11) · informer source (Tasks 2-4) · config+validation (Task 5) · buildSources (Task 6) · `OnPollComplete`/status writeback (Tasks 7-10) · manifests+Helm (Task 11) · docs (Task 12) · unit (fake client, Tasks 2-9) + k3s integration (Task 13) · acceptance gates (Task 14). All issue #160 acceptance-criteria boxes map to a task.
- **Type consistency:** `feedGVR`, `KubernetesOptions`, `KubernetesFeedSourceConfig`, `newKubernetesWithClient`, `NewKubernetes`, `ReportPoll`, and the `OnPollComplete func(feedURL string, changeCount int, err error, when time.Time)` signature are used identically across Tasks 2-10. `namespacedName` (not `types.Namespaced`) is the index value type — see the explicit note in Task 2 Step 3.
- **Lease gating:** status writeback rides `OnPollComplete`, which only fires for feeds this replica actually polled (coordinator `TryAcquire` gate in the pipeline), so no cross-replica write contention — no extra leadership check needed.
- **YAGNI:** no finalizers, webhooks, or multi-version conversion; single served `v1`; the source only reads feeds and writes status.
```