package telemetry

import (
	"bufio"
	"bytes"
	"context"
	"net"
	"sort"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"

	"github.com/iambod/rss2msg/internal/config"
)

// carbonSink is a throwaway TCP listener that accepts a single Carbon plaintext
// connection and collects every line written to it. The graphite exporter dials
// and closes per Export, so reading until EOF yields the full payload.
type carbonSink struct {
	addr  string
	lines chan []string
	ln    net.Listener
}

func newCarbonSink(t *testing.T) *carbonSink {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &carbonSink{addr: ln.Addr().String(), lines: make(chan []string, 1), ln: ln}
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		var got []string
		sc := bufio.NewScanner(conn)
		for sc.Scan() {
			if line := sc.Text(); line != "" {
				got = append(got, line)
			}
		}
		s.lines <- got
	}()
	t.Cleanup(func() { _ = ln.Close() })
	return s
}

// collect waits for the sink to observe the exporter's payload.
func (s *carbonSink) collect(t *testing.T) []string {
	t.Helper()
	select {
	case got := <-s.lines:
		sort.Strings(got)
		return got
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for carbon payload")
		return nil
	}
}

func exportOnce(t *testing.T, addr, prefix string, m metricdata.Metrics) {
	t.Helper()
	exp, err := newGraphiteExporter(config.TelemetryGraphiteConfig{Address: addr, Prefix: prefix})
	if err != nil {
		t.Fatalf("newGraphiteExporter: %v", err)
	}
	rm := &metricdata.ResourceMetrics{
		ScopeMetrics: []metricdata.ScopeMetrics{{Metrics: []metricdata.Metrics{m}}},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := exp.Export(ctx, rm); err != nil {
		t.Fatalf("Export: %v", err)
	}
}

var fixedTime = time.Unix(1700000000, 0)

func TestGraphiteExporterEncodesSum(t *testing.T) {
	t.Parallel()
	sink := newCarbonSink(t)
	exportOnce(t, sink.addr, "rss2msg", metricdata.Metrics{
		Name: "feed.fetches",
		Data: metricdata.Sum[int64]{
			DataPoints: []metricdata.DataPoint[int64]{
				{Value: 5, Time: fixedTime},
			},
		},
	})
	got := sink.collect(t)
	want := []string{"rss2msg.feed.fetches 5 1700000000"}
	if !equalLines(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestGraphiteExporterEncodesGaugeFloat(t *testing.T) {
	t.Parallel()
	sink := newCarbonSink(t)
	exportOnce(t, sink.addr, "rss2msg", metricdata.Metrics{
		Name: "queue.depth",
		Data: metricdata.Gauge[float64]{
			DataPoints: []metricdata.DataPoint[float64]{
				{Value: 3.5, Time: fixedTime},
			},
		},
	})
	got := sink.collect(t)
	want := []string{"rss2msg.queue.depth 3.5 1700000000"}
	if !equalLines(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestGraphiteExporterFoldsAttributesAsTags(t *testing.T) {
	t.Parallel()
	sink := newCarbonSink(t)
	exportOnce(t, sink.addr, "rss2msg", metricdata.Metrics{
		Name: "sink.publish.failures",
		Data: metricdata.Sum[int64]{
			DataPoints: []metricdata.DataPoint[int64]{
				{
					Value: 2,
					Time:  fixedTime,
					Attributes: attribute.NewSet(
						attribute.String("sink", "kafka"),
						attribute.String("reason", "timed out"),
					),
				},
			},
		},
	})
	got := sink.collect(t)
	// Tags are sorted by key; value spaces are sanitized to underscores.
	want := []string{"rss2msg.sink.publish.failures;reason=timed_out;sink=kafka 2 1700000000"}
	if !equalLines(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestGraphiteExporterEncodesHistogram(t *testing.T) {
	t.Parallel()
	sink := newCarbonSink(t)
	exportOnce(t, sink.addr, "rss2msg", metricdata.Metrics{
		Name: "feed.fetch.duration",
		Data: metricdata.Histogram[float64]{
			DataPoints: []metricdata.HistogramDataPoint[float64]{
				{
					Count: 4,
					Sum:   40,
					Min:   metricdata.NewExtrema[float64](2),
					Max:   metricdata.NewExtrema[float64](20),
					Time:  fixedTime,
				},
			},
		},
	})
	got := sink.collect(t)
	want := []string{
		"rss2msg.feed.fetch.duration.count 4 1700000000",
		"rss2msg.feed.fetch.duration.max 20 1700000000",
		"rss2msg.feed.fetch.duration.min 2 1700000000",
		"rss2msg.feed.fetch.duration.sum 40 1700000000",
	}
	if !equalLines(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestGraphiteExporterAddsResourceInstanceIDTag(t *testing.T) {
	t.Parallel()
	// Two replicas pushing identical tag sets would merge into one Carbon series;
	// the resource's service.instance.id must be folded in to keep them distinct.
	sink := newCarbonSink(t)
	exp, err := newGraphiteExporter(config.TelemetryGraphiteConfig{Address: sink.addr, Prefix: "rss2msg"})
	if err != nil {
		t.Fatalf("newGraphiteExporter: %v", err)
	}
	rm := &metricdata.ResourceMetrics{
		Resource: resource.NewSchemaless(semconv.ServiceInstanceID("host-a")),
		ScopeMetrics: []metricdata.ScopeMetrics{{Metrics: []metricdata.Metrics{{
			Name: "feed.fetches",
			Data: metricdata.Sum[int64]{DataPoints: []metricdata.DataPoint[int64]{{
				Value:      5,
				Time:       fixedTime,
				Attributes: attribute.NewSet(attribute.String("feed_url", "https://example.com")),
			}}},
		}}}},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := exp.Export(ctx, rm); err != nil {
		t.Fatalf("Export: %v", err)
	}
	got := sink.collect(t)
	// Tags are sorted by key: feed_url < service.instance.id.
	want := []string{"rss2msg.feed.fetches;feed_url=https://example.com;service.instance.id=host-a 5 1700000000"}
	if !equalLines(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestGraphiteExporterEmptyPrefixOmitsLeadingDot(t *testing.T) {
	t.Parallel()
	sink := newCarbonSink(t)
	exportOnce(t, sink.addr, "", metricdata.Metrics{
		Name: "feed.changes",
		Data: metricdata.Sum[int64]{
			DataPoints: []metricdata.DataPoint[int64]{{Value: 1, Time: fixedTime}},
		},
	})
	got := sink.collect(t)
	want := []string{"feed.changes 1 1700000000"}
	if !equalLines(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestNewGraphiteExporterRequiresAddress(t *testing.T) {
	t.Parallel()
	if _, err := newGraphiteExporter(config.TelemetryGraphiteConfig{}); err == nil {
		t.Fatal("expected error when address is empty")
	}
}

func TestGraphiteExporterExportAfterShutdownIsNoop(t *testing.T) {
	t.Parallel()
	exp, err := newGraphiteExporter(config.TelemetryGraphiteConfig{Address: "127.0.0.1:1"})
	if err != nil {
		t.Fatalf("newGraphiteExporter: %v", err)
	}
	if err := exp.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	// After shutdown Export must not attempt to dial the (unreachable) address.
	rm := &metricdata.ResourceMetrics{
		ScopeMetrics: []metricdata.ScopeMetrics{{Metrics: []metricdata.Metrics{{
			Name: "feed.fetches",
			Data: metricdata.Sum[int64]{DataPoints: []metricdata.DataPoint[int64]{{Value: 1, Time: fixedTime}}},
		}}}},
	}
	if err := exp.Export(context.Background(), rm); err != nil {
		t.Fatalf("Export after shutdown should be a noop, got %v", err)
	}
}

func TestSetupWiresGraphiteAndFlushesOnShutdown(t *testing.T) {
	t.Parallel()
	sink := newCarbonSink(t)

	cfg := config.Defaults()
	cfg.Telemetry.Metrics.Enabled = true
	cfg.Telemetry.Graphite.Enabled = true
	cfg.Telemetry.Graphite.Address = sink.addr
	cfg.Telemetry.Graphite.Prefix = "rss2msg"

	tel, err := Setup(context.Background(), cfg, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}

	ctr, err := tel.Meter.Int64Counter("feed.fetches")
	if err != nil {
		t.Fatalf("counter: %v", err)
	}
	ctr.Add(context.Background(), 7)

	// Shutdown forces a final collect + export to the Carbon endpoint.
	if err := tel.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	got := sink.collect(t)
	var found bool
	for _, l := range got {
		// Setup populates the resource's service.instance.id, so the line carries
		// it as a tag: "rss2msg.feed.fetches;service.instance.id=<host> 7 <ts>".
		trimmed := strings.TrimSpace(l)
		if strings.HasPrefix(trimmed, "rss2msg.feed.fetches;") && strings.Contains(trimmed, "service.instance.id=") {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected rss2msg.feed.fetches line with instance id tag, got %v", got)
	}
}

func equalLines(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if strings.TrimSpace(got[i]) != want[i] {
			return false
		}
	}
	return true
}
