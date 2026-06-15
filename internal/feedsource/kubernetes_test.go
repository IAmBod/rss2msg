package feedsource

import (
	"testing"
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
