package feedsource

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic/fake"
)

// feedObject builds the unstructured shape an informer delivers for a Feed CR:
// the full object map with apiVersion/kind/metadata/spec keys. spec is supplied
// by the caller so each test controls only the part it exercises.
func feedObject(name string, spec map[string]any) map[string]any {
	return map[string]any{
		"apiVersion": "rss2msg.io/v1",
		"kind":       "Feed",
		"metadata":   map[string]any{"name": name, "namespace": "feeds"},
		"spec":       spec,
	}
}

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

func TestKubernetesSpecFromUnstructured(t *testing.T) {
	// Arrays and nested objects arrive as []interface{} / map[string]interface{}
	// from the dynamic informer, never as concrete []string / typed structs.
	obj := feedObject("hn", map[string]any{
		"url":      "https://news.ycombinator.com/rss",
		"interval": "5m",
		"sinks":    []any{"kafka-main", "stdout"},
		"http": map[string]any{
			"timeout": "10s",
			"headers": map[string]any{"User-Agent": "rss2msg"},
		},
	})

	spec, err := specFromUnstructured(obj)
	if err != nil {
		t.Fatalf("specFromUnstructured: %v", err)
	}
	if spec.URL != "https://news.ycombinator.com/rss" {
		t.Errorf("URL = %q", spec.URL)
	}
	if spec.Interval != "5m" {
		t.Errorf("Interval = %q", spec.Interval)
	}
	if len(spec.Sinks) != 2 || spec.Sinks[0] != "kafka-main" || spec.Sinks[1] != "stdout" {
		t.Errorf("Sinks = %+v", spec.Sinks)
	}
	if spec.HTTP == nil {
		t.Fatalf("HTTP = nil, want populated")
	}
	if spec.HTTP.Timeout != "10s" {
		t.Errorf("HTTP.Timeout = %q", spec.HTTP.Timeout)
	}
	if spec.HTTP.Headers["User-Agent"] != "rss2msg" {
		t.Errorf("HTTP.Headers = %+v", spec.HTTP.Headers)
	}
}

func TestKubernetesSourceEvictsStaleIndexOnURLChange(t *testing.T) {
	scheme := runtime.NewScheme()
	gvrToKind := map[schema.GroupVersionResource]string{feedGVR: "FeedList"}
	client := fake.NewSimpleDynamicClientWithCustomListKinds(scheme, gvrToKind,
		unstructuredFeed("a", map[string]any{"url": "https://e/old"}),
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, err := newKubernetesWithClient(ctx, "k8s", client, KubernetesOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })

	_, err = client.Resource(feedGVR).Namespace("feeds").Update(ctx,
		unstructuredFeed("a", map[string]any{"url": "https://e/new"}), metav1.UpdateOptions{})
	if err != nil {
		t.Fatal(err)
	}

	deadline := time.After(2 * time.Second)
	for {
		s.mu.RLock()
		_, oldStale := s.index["https://e/old"]
		_, newPresent := s.index["https://e/new"]
		s.mu.RUnlock()
		if !oldStale && newPresent {
			return // converged: old evicted, new present
		}
		select {
		case <-deadline:
			t.Fatalf("index did not converge: oldStale=%v newPresent=%v", oldStale, newPresent)
		case <-time.After(20 * time.Millisecond):
		}
	}
}

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

	// drain any initial-sync signal
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

func TestNewKubernetesBadKubeconfig(t *testing.T) {
	ctx := context.Background()
	_, err := NewKubernetes(ctx, KubernetesOptions{Name: "k8s"}, "/nonexistent/kubeconfig")
	if err == nil {
		t.Fatal("expected an error for a missing kubeconfig path")
	}
}
