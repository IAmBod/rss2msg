package feedsource

import (
	"fmt"
	"strings"
)

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
