//go:build integration

package kubernetes_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/k3s"

	"github.com/iambod/rss2msg/internal/feedsource/sources/kubernetes"
)

var (
	crdGVR    = schema.GroupVersionResource{Group: "apiextensions.k8s.io", Version: "v1", Resource: "customresourcedefinitions"}
	itFeedGVR = schema.GroupVersionResource{Group: "rss2msg.io", Version: "v1", Resource: "feeds"}
)

func TestKubernetesSourceK3sRoundTrip(t *testing.T) {
	ctx := context.Background()

	k3sC, err := k3s.Run(ctx, "rancher/k3s:v1.31.2-k3s1")
	if err != nil {
		t.Fatalf("start k3s: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(k3sC) })

	kubeBytes, err := k3sC.GetKubeConfig(ctx)
	if err != nil {
		t.Fatalf("kubeconfig: %v", err)
	}

	restcfg, err := clientcmd.RESTConfigFromKubeConfig(kubeBytes)
	if err != nil {
		t.Fatalf("rest config: %v", err)
	}
	dyn, err := dynamic.NewForConfig(restcfg)
	if err != nil {
		t.Fatalf("dynamic client: %v", err)
	}

	crd := loadYAMLDoc(t, crdManifestPath)
	if _, err := dyn.Resource(crdGVR).Create(ctx, crd, metav1.CreateOptions{}); err != nil {
		t.Fatalf("apply CRD: %v", err)
	}

	waitFor(t, 60*time.Second, func() bool {
		got, err := dyn.Resource(crdGVR).Get(ctx, "feeds.rss2msg.io", metav1.GetOptions{})
		if err != nil {
			return false
		}
		conds, _, _ := unstructured.NestedSlice(got.Object, "status", "conditions")
		for _, c := range conds {
			m, ok := c.(map[string]any)
			if ok && m["type"] == "Established" && m["status"] == "True" {
				return true
			}
		}
		return false
	})

	feed := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "rss2msg.io/v1",
		"kind":       "Feed",
		"metadata":   map[string]any{"name": "hn", "namespace": "default"},
		"spec":       map[string]any{"url": "https://e/hn", "interval": "5m"},
	}}
	if _, err := dyn.Resource(itFeedGVR).Namespace("default").Create(ctx, feed, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create Feed: %v", err)
	}

	kubePath := filepath.Join(t.TempDir(), "kubeconfig")
	if err := os.WriteFile(kubePath, kubeBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	src, err := kubernetes.NewKubernetes(ctx, kubernetes.KubernetesOptions{Name: "k8s", Namespace: "default", WriteStatus: true}, kubePath)
	if err != nil {
		t.Fatalf("NewKubernetes: %v", err)
	}
	t.Cleanup(func() { _ = src.Close() })

	waitFor(t, 30*time.Second, func() bool {
		feeds, err := src.Feeds(ctx)
		if err != nil {
			return false
		}
		for _, f := range feeds {
			if f.URL == "https://e/hn" {
				return true
			}
		}
		return false
	})

	src.ReportPoll(ctx, "https://e/hn", 2, nil, time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC))
	waitFor(t, 30*time.Second, func() bool {
		got, err := dyn.Resource(itFeedGVR).Namespace("default").Get(ctx, "hn", metav1.GetOptions{})
		if err != nil {
			return false
		}
		n, found, _ := unstructured.NestedInt64(got.Object, "status", "lastChangeCount")
		return found && n == 2
	})
}

func loadYAMLDoc(t *testing.T, path string) *unstructured.Unstructured {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	m := map[string]any{}
	if err := utilyaml.NewYAMLOrJSONDecoder(bytes.NewReader(raw), 4096).Decode(&m); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
	return &unstructured.Unstructured{Object: m}
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		if cond() {
			return
		}
		select {
		case <-deadline:
			t.Fatal("condition not met within timeout")
		case <-time.After(500 * time.Millisecond):
		}
	}
}
