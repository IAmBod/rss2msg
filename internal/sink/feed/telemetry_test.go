package feed

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestTelemetry_RequestsCounted(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	instr, err := newInstruments(mp.Meter("test"))
	if err != nil {
		t.Fatal(err)
	}
	h := newTestHandler(t)
	h.instr = instr
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/rss", nil))

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	if len(rm.ScopeMetrics) == 0 {
		t.Fatal("expected at least one metric recorded")
	}
}

// findSum looks up the first Sum[int64] metric with the given name across all
// scopes and returns its data points. It fails the test if the metric is not
// found or is the wrong aggregation type.
func findSum(t *testing.T, rm metricdata.ResourceMetrics, name string) []metricdata.DataPoint[int64] {
	t.Helper()
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			s, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("metric %q: expected Sum[int64], got %T", name, m.Data)
			}
			return s.DataPoints
		}
	}
	t.Fatalf("metric %q not found in collected metrics", name)
	return nil
}

// assertAttr checks that dp.Attributes contains the key=value pair.
func assertAttr(t *testing.T, dp metricdata.DataPoint[int64], key, want string) {
	t.Helper()
	v, ok := dp.Attributes.Value(attribute.Key(key))
	if !ok {
		t.Fatalf("attribute %q missing from data point (attributes: %v)", key, dp.Attributes.ToSlice())
	}
	if got := v.AsString(); got != want {
		t.Fatalf("attribute %q: got %q, want %q", key, got, want)
	}
}

// collectMetrics is a convenience helper that collects the current SDK metrics.
func collectMetrics(t *testing.T, reader *sdkmetric.ManualReader) metricdata.ResourceMetrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	return rm
}

func TestTelemetry_AuthMetricAttributes(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	instr, err := newInstruments(mp.Meter("test"))
	if err != nil {
		t.Fatal(err)
	}
	h := newTestHandler(t)
	h.instr = instr
	h.cfg.rssAuth = &SurfaceAuth{BearerTokens: []NamedSecret{{Name: "mytoken", Secret: "s3cr3t"}}}

	// --- success: correct bearer token ---
	req := httptest.NewRequest(http.MethodGet, "/rss", nil)
	req.Header.Set("Authorization", "Bearer s3cr3t")
	h.ServeHTTP(httptest.NewRecorder(), req)

	rm := collectMetrics(t, reader)
	successPoints := findSum(t, rm, "feed_sink.auth_success")
	if len(successPoints) == 0 {
		t.Fatal("expected at least one auth_success data point")
	}
	dp := successPoints[0]
	assertAttr(t, dp, "surface", "rss")
	assertAttr(t, dp, "credential", "mytoken")

	// --- failure: no credentials ---
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/rss", nil))

	rm = collectMetrics(t, reader)
	failPoints := findSum(t, rm, "feed_sink.auth_failure")
	// find the data point with reason=no_credentials
	var noCredDP *metricdata.DataPoint[int64]
	for i := range failPoints {
		v, ok := failPoints[i].Attributes.Value(attribute.Key("reason"))
		if ok && v.AsString() == "no_credentials" {
			noCredDP = &failPoints[i]
			break
		}
	}
	if noCredDP == nil {
		t.Fatal("expected auth_failure data point with reason=no_credentials")
	}
	assertAttr(t, *noCredDP, "surface", "rss")

	// --- failure: wrong bearer token ---
	wrongReq := httptest.NewRequest(http.MethodGet, "/rss", nil)
	wrongReq.Header.Set("Authorization", "Bearer wrongtoken")
	h.ServeHTTP(httptest.NewRecorder(), wrongReq)

	rm = collectMetrics(t, reader)
	failPoints = findSum(t, rm, "feed_sink.auth_failure")
	var badTokenDP *metricdata.DataPoint[int64]
	for i := range failPoints {
		v, ok := failPoints[i].Attributes.Value(attribute.Key("reason"))
		if ok && v.AsString() == "bad_token" {
			badTokenDP = &failPoints[i]
			break
		}
	}
	if badTokenDP == nil {
		t.Fatal("expected auth_failure data point with reason=bad_token")
	}
	assertAttr(t, *badTokenDP, "surface", "rss")
}
