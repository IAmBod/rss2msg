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

	var b strings.Builder
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			e.encode(&b, m)
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

func (e *graphiteExporter) encode(b *strings.Builder, m metricdata.Metrics) {
	switch data := m.Data.(type) {
	case metricdata.Sum[int64]:
		encodeNum(e, b, m.Name, data.DataPoints)
	case metricdata.Sum[float64]:
		encodeNum(e, b, m.Name, data.DataPoints)
	case metricdata.Gauge[int64]:
		encodeNum(e, b, m.Name, data.DataPoints)
	case metricdata.Gauge[float64]:
		encodeNum(e, b, m.Name, data.DataPoints)
	case metricdata.Histogram[int64]:
		encodeHist(e, b, m.Name, data.DataPoints)
	case metricdata.Histogram[float64]:
		encodeHist(e, b, m.Name, data.DataPoints)
	}
}

func encodeNum[N int64 | float64](e *graphiteExporter, b *strings.Builder, name string, dps []metricdata.DataPoint[N]) {
	for _, dp := range dps {
		e.line(b, name, dp.Attributes, float64(dp.Value), dp.Time)
	}
}

func encodeHist[N int64 | float64](e *graphiteExporter, b *strings.Builder, name string, dps []metricdata.HistogramDataPoint[N]) {
	for _, dp := range dps {
		e.line(b, name+".count", dp.Attributes, float64(dp.Count), dp.Time)
		e.line(b, name+".sum", dp.Attributes, float64(dp.Sum), dp.Time)
		if v, ok := dp.Min.Value(); ok {
			e.line(b, name+".min", dp.Attributes, float64(v), dp.Time)
		}
		if v, ok := dp.Max.Value(); ok {
			e.line(b, name+".max", dp.Attributes, float64(v), dp.Time)
		}
	}
}

// line appends a single Carbon plaintext record. Attributes fold into Graphite
// tags ("path;key=value;…"), sorted by key for deterministic output.
func (e *graphiteExporter) line(b *strings.Builder, name string, attrs attribute.Set, value float64, ts time.Time) {
	b.WriteString(metricPath(e.prefix, name))
	writeTags(b, attrs)
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

func writeTags(b *strings.Builder, attrs attribute.Set) {
	if attrs.Len() == 0 {
		return
	}
	type tag struct{ k, v string }
	tags := make([]tag, 0, attrs.Len())
	for it := attrs.Iter(); it.Next(); {
		kv := it.Attribute()
		tags = append(tags, tag{k: sanitizeTag(string(kv.Key)), v: sanitizeTag(kv.Value.String())})
	}
	sort.Slice(tags, func(i, j int) bool { return tags[i].k < tags[j].k })
	for _, t := range tags {
		b.WriteByte(';')
		b.WriteString(t.k)
		b.WriteByte('=')
		b.WriteString(t.v)
	}
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
