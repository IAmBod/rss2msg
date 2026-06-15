package feedsource

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/tools/cache"

	"github.com/iambod/rss2msg/internal/config"
)

var _ Source = (*Kubernetes)(nil)

// feedGVR is the GroupVersionResource for the Feed custom resource.
var feedGVR = schema.GroupVersionResource{Group: "rss2msg.io", Version: "v1", Resource: "feeds"}

const defaultResyncInterval = 10 * time.Minute

// namespacedName identifies a Feed CR for status writeback.
type namespacedName struct{ Namespace, Name string }

// KubernetesOptions configures a Kubernetes-backed feed source. The source
// watches Feed custom resources; it never creates or deletes them.
type KubernetesOptions struct {
	Name           string
	Namespace      string        // "" = all namespaces (cluster-wide watch)
	LabelSelector  string        // optional; scope which Feeds this instance owns
	ResyncInterval time.Duration // default 10m
	WriteStatus    bool          // enable .status writeback (used by a later task)
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
	stopOnce sync.Once

	changes chan struct{}

	mu    sync.RWMutex
	specs map[string]FeedSpec       // key: namespace/name
	index map[string]namespacedName // key: feed URL -> {namespace, name}
}

// newKubernetesWithClient builds a source from an injected dynamic client (used
// by both NewKubernetes and tests). It starts the informer and blocks until the
// initial cache sync completes.
func newKubernetesWithClient(ctx context.Context, name string, client dynamic.Interface, opts KubernetesOptions) (*Kubernetes, error) {
	resync := opts.ResyncInterval
	if resync <= 0 {
		resync = defaultResyncInterval
	}
	tweak := func(lo *metav1.ListOptions) {
		if opts.LabelSelector != "" {
			lo.LabelSelector = opts.LabelSelector
		}
	}
	factory := dynamicinformer.NewFilteredDynamicSharedInformerFactory(client, resync, opts.Namespace, tweak)
	informer := factory.ForResource(feedGVR).Informer()

	k := &Kubernetes{
		name:        name,
		client:      client,
		namespace:   opts.Namespace,
		writeStatus: opts.WriteStatus,
		factory:     factory,
		informer:    informer,
		stop:        make(chan struct{}),
		changes:     make(chan struct{}, 1),
		specs:       map[string]FeedSpec{},
		index:       map[string]namespacedName{},
	}

	if _, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { k.upsert(obj); k.signal() },
		UpdateFunc: func(_, obj any) { k.upsert(obj); k.signal() },
		DeleteFunc: func(obj any) { k.remove(obj); k.signal() },
	}); err != nil {
		return nil, fmt.Errorf("kubernetes feed source %q: add handler: %w", name, err)
	}

	factory.Start(k.stop)
	if !cache.WaitForCacheSync(ctx.Done(), informer.HasSynced) {
		_ = k.Close()
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
	k.stopOnce.Do(func() { close(k.stop) })
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
	k.index[spec.URL] = namespacedName{Namespace: u.GetNamespace(), Name: u.GetName()}
	k.mu.Unlock()
}

func (k *Kubernetes) remove(obj any) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		tomb, isTomb := obj.(cache.DeletedFinalStateUnknown)
		if !isTomb {
			return
		}
		u, ok = tomb.Obj.(*unstructured.Unstructured)
		if !ok {
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

// specFromUnstructured maps a Feed custom resource (the unstructured object an
// informer delivers) to a FeedSpec. Only spec.{url,interval,sinks,http} are
// consulted; url is required. Values arrive in dynamic-client form: arrays as
// []any and nested objects as map[string]any.
func specFromUnstructured(obj map[string]any) (FeedSpec, error) {
	spec, _ := obj["spec"].(map[string]any)

	url := objString(spec, "url")
	if strings.TrimSpace(url) == "" {
		return FeedSpec{}, fmt.Errorf("spec.url is required")
	}

	out := FeedSpec{
		URL:      url,
		Interval: strings.TrimSpace(objString(spec, "interval")),
		Sinks:    objStringSlice(spec, "sinks"),
	}

	if http, ok := spec["http"].(map[string]any); ok {
		out.HTTP = &FeedSpecHTTP{
			Timeout: strings.TrimSpace(objString(http, "timeout")),
			Headers: objStringMap(http, "headers"),
		}
	}
	return out, nil
}

// objString reads a string field; missing/non-string reads as "".
func objString(m map[string]any, key string) string {
	s, _ := m[key].(string)
	return s
}

// objStringSlice reads a []any of strings (the dynamic-client array shape).
// Non-string elements are skipped; missing reads as nil.
func objStringSlice(m map[string]any, key string) []string {
	raw, ok := m[key].([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(raw))
	for _, e := range raw {
		if s, ok := e.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

// objStringMap reads a map[string]any of string values; missing reads as nil.
func objStringMap(m map[string]any, key string) map[string]string {
	raw, ok := m[key].(map[string]any)
	if !ok {
		return nil
	}
	out := make(map[string]string, len(raw))
	for k, v := range raw {
		if s, ok := v.(string); ok {
			out[k] = s
		}
	}
	return out
}
