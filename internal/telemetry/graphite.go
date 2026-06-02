package telemetry

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"

	"github.com/iambod/rss2msg/internal/config"
)

// graphiteExporter is an OTEL metric Exporter that speaks the Carbon plaintext
// protocol. Each export opens a short-lived TCP connection to the Carbon
// endpoint, writes one "<path> <value> <unix-seconds>\n" line per data point,
// and closes it. Cumulative temporality is used (matching Prometheus
// semantics); Graphite derives rates with nonNegativeDerivative at query time.
type graphiteExporter struct {
	address string
	prefix  string
	dialer  func(ctx context.Context) (net.Conn, error)

	mu       sync.Mutex
	shutdown bool
}

// newGraphiteExporter builds a Carbon exporter from config. Address is required.
func newGraphiteExporter(cfg config.TelemetryGraphiteConfig) (*graphiteExporter, error) {
	if strings.TrimSpace(cfg.Address) == "" {
		return nil, errors.New("graphite address is required")
	}
	e := &graphiteExporter{
		address: cfg.Address,
		prefix:  strings.Trim(cfg.Prefix, "."),
	}
	e.dialer = func(ctx context.Context) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "tcp", e.address)
	}
	return e, nil
}

// Temporality reports the temporality for an instrument kind. Cumulative is the
// SDK default and the right fit for Graphite.
func (e *graphiteExporter) Temporality(k sdkmetric.InstrumentKind) metricdata.Temporality {
	return sdkmetric.DefaultTemporalitySelector(k)
}

// Aggregation reports the aggregation for an instrument kind.
func (e *graphiteExporter) Aggregation(k sdkmetric.InstrumentKind) sdkmetric.Aggregation {
	return sdkmetric.DefaultAggregationSelector(k)
}

// Export serializes rm to Carbon plaintext and pushes it to the endpoint.
func (e *graphiteExporter) Export(ctx context.Context, rm *metricdata.ResourceMetrics) error {
	e.mu.Lock()
	down := e.shutdown
	e.mu.Unlock()
	if down {
		return nil
	}

	// Fold the resource's instance id into every line's tags so two replicas
	// pushing the same path+tags don't merge into one Carbon series.
	extra := resourceTags(rm.Resource)
	var b strings.Builder
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			e.encode(&b, m, extra)
		}
	}
	payload := b.String()
	if payload == "" {
		return nil
	}

	conn, err := e.dialer(ctx)
	if err != nil {
		return fmt.Errorf("graphite dial %s: %w", e.address, err)
	}
	defer func() { _ = conn.Close() }()
	if dl, ok := ctx.Deadline(); ok {
		_ = conn.SetWriteDeadline(dl)
	}
	if _, err := io.WriteString(conn, payload); err != nil {
		return fmt.Errorf("graphite write: %w", err)
	}
	return nil
}

// ForceFlush is a no-op: Export pushes synchronously and holds no buffer.
func (e *graphiteExporter) ForceFlush(context.Context) error { return nil }

// Shutdown marks the exporter stopped; subsequent Export calls are no-ops.
func (e *graphiteExporter) Shutdown(context.Context) error {
	e.mu.Lock()
	e.shutdown = true
	e.mu.Unlock()
	return nil
}

func (e *graphiteExporter) encode(b *strings.Builder, m metricdata.Metrics, extra []graphiteTag) {
	switch data := m.Data.(type) {
	case metricdata.Sum[int64]:
		encodeNum(e, b, m.Name, data.DataPoints, extra)
	case metricdata.Sum[float64]:
		encodeNum(e, b, m.Name, data.DataPoints, extra)
	case metricdata.Gauge[int64]:
		encodeNum(e, b, m.Name, data.DataPoints, extra)
	case metricdata.Gauge[float64]:
		encodeNum(e, b, m.Name, data.DataPoints, extra)
	case metricdata.Histogram[int64]:
		encodeHist(e, b, m.Name, data.DataPoints, extra)
	case metricdata.Histogram[float64]:
		encodeHist(e, b, m.Name, data.DataPoints, extra)
	}
}

func encodeNum[N int64 | float64](e *graphiteExporter, b *strings.Builder, name string, dps []metricdata.DataPoint[N], extra []graphiteTag) {
	for _, dp := range dps {
		e.line(b, name, dp.Attributes, extra, float64(dp.Value), dp.Time)
	}
}

func encodeHist[N int64 | float64](e *graphiteExporter, b *strings.Builder, name string, dps []metricdata.HistogramDataPoint[N], extra []graphiteTag) {
	for _, dp := range dps {
		e.line(b, name+".count", dp.Attributes, extra, float64(dp.Count), dp.Time)
		e.line(b, name+".sum", dp.Attributes, extra, float64(dp.Sum), dp.Time)
		if v, ok := dp.Min.Value(); ok {
			e.line(b, name+".min", dp.Attributes, extra, float64(v), dp.Time)
		}
		if v, ok := dp.Max.Value(); ok {
			e.line(b, name+".max", dp.Attributes, extra, float64(v), dp.Time)
		}
	}
}

// line appends a single Carbon plaintext record. Attributes fold into Graphite
// tags ("path;key=value;…"), sorted by key for deterministic output.
func (e *graphiteExporter) line(b *strings.Builder, name string, attrs attribute.Set, extra []graphiteTag, value float64, ts time.Time) {
	b.WriteString(metricPath(e.prefix, name))
	writeTags(b, attrs, extra)
	b.WriteByte(' ')
	b.WriteString(strconv.FormatFloat(value, 'g', -1, 64))
	b.WriteByte(' ')
	b.WriteString(strconv.FormatInt(ts.Unix(), 10))
	b.WriteByte('\n')
}

func metricPath(prefix, name string) string {
	name = sanitizeNode(name)
	if prefix == "" {
		return name
	}
	return prefix + "." + name
}

// graphiteTag is a raw (unsanitized) tag key/value carried from the resource
// into writeTags, which sanitizes on output alongside the data-point attributes.
type graphiteTag struct{ k, v string }

func writeTags(b *strings.Builder, attrs attribute.Set, extra []graphiteTag) {
	if attrs.Len() == 0 && len(extra) == 0 {
		return
	}
	merged := make(map[string]string, attrs.Len()+len(extra))
	// Resource extras are lower priority; data-point attributes overwrite them.
	for _, t := range extra {
		merged[sanitizeTag(t.k)] = sanitizeTag(t.v)
	}
	for it := attrs.Iter(); it.Next(); {
		kv := it.Attribute()
		merged[sanitizeTag(string(kv.Key))] = sanitizeTag(kv.Value.String())
	}
	keys := make([]string, 0, len(merged))
	for k := range merged {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		b.WriteByte(';')
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(merged[k])
	}
}

// resourceTags extracts service.instance.id from the OTEL resource as a Graphite
// tag. Without it, replicas pushing the same path+tags collide into one series.
func resourceTags(res *resource.Resource) []graphiteTag {
	if res == nil {
		return nil
	}
	for it := res.Iter(); it.Next(); {
		if a := it.Attribute(); a.Key == semconv.ServiceInstanceIDKey {
			return []graphiteTag{{k: string(a.Key), v: a.Value.String()}}
		}
	}
	return nil
}

// sanitizeNode keeps a metric-path node Carbon-safe: spaces become underscores
// and the tag/value separators are stripped. Dots are preserved because they
// form the Carbon hierarchy.
func sanitizeNode(s string) string {
	return strings.Map(func(r rune) rune {
		switch r {
		case ' ', '\t', '\n':
			return '_'
		case ';', '=':
			return '_'
		default:
			return r
		}
	}, s)
}

// sanitizeTag keeps a tag key or value Carbon-safe. Dots are allowed in tag
// values but spaces and the ';'/'=' separators are not.
func sanitizeTag(s string) string {
	s = strings.Map(func(r rune) rune {
		switch r {
		case ' ', '\t', '\n', ';', '=':
			return '_'
		default:
			return r
		}
	}, s)
	if s == "" {
		return "_"
	}
	return s
}
