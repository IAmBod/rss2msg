package telemetry

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/iambod/rss2msg/internal/config"
)

// fakeCWMetrics records PutMetricData calls so tests can assert what the
// exporter produced without contacting AWS.
type fakeCWMetrics struct {
	mu    sync.Mutex
	calls []*cloudwatch.PutMetricDataInput
	err   error
}

func (f *fakeCWMetrics) PutMetricData(_ context.Context, in *cloudwatch.PutMetricDataInput, _ ...func(*cloudwatch.Options)) (*cloudwatch.PutMetricDataOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	f.calls = append(f.calls, in)
	return &cloudwatch.PutMetricDataOutput{}, nil
}

var cwFixedTime = time.Unix(1700000000, 0)

func exportMetric(t *testing.T, f *fakeCWMetrics, namespace string, m metricdata.Metrics) {
	t.Helper()
	exp := newCloudWatchMetricsExporter(f, namespace)
	rm := &metricdata.ResourceMetrics{
		ScopeMetrics: []metricdata.ScopeMetrics{{Metrics: []metricdata.Metrics{m}}},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := exp.Export(ctx, rm); err != nil {
		t.Fatalf("Export: %v", err)
	}
}

func (f *fakeCWMetrics) allData() []cwDatumView {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []cwDatumView
	for _, c := range f.calls {
		for _, d := range c.MetricData {
			v := cwDatumView{namespace: *c.Namespace, name: *d.MetricName}
			if d.Value != nil {
				v.value = *d.Value
				v.hasValue = true
			}
			for _, dim := range d.Dimensions {
				v.dims = append(v.dims, *dim.Name+"="+*dim.Value)
			}
			out = append(out, v)
		}
	}
	return out
}

type cwDatumView struct {
	namespace string
	name      string
	value     float64
	hasValue  bool
	dims      []string
}

func TestCloudWatchMetricsExportsSum(t *testing.T) {
	t.Parallel()
	f := &fakeCWMetrics{}
	exportMetric(t, f, "rss2msg", metricdata.Metrics{
		Name: "feed.fetches",
		Data: metricdata.Sum[int64]{
			DataPoints: []metricdata.DataPoint[int64]{{Value: 5, Time: cwFixedTime}},
		},
	})
	data := f.allData()
	if len(data) != 1 {
		t.Fatalf("expected 1 datum, got %d", len(data))
	}
	if data[0].namespace != "rss2msg" || data[0].name != "feed.fetches" || !data[0].hasValue || data[0].value != 5 {
		t.Fatalf("unexpected datum: %+v", data[0])
	}
}

func TestCloudWatchMetricsExportsHistogramAsStatisticSet(t *testing.T) {
	t.Parallel()
	f := &fakeCWMetrics{}
	exp := newCloudWatchMetricsExporter(f, "rss2msg")
	rm := &metricdata.ResourceMetrics{ScopeMetrics: []metricdata.ScopeMetrics{{Metrics: []metricdata.Metrics{{
		Name: "feed.fetch.duration",
		Data: metricdata.Histogram[float64]{
			DataPoints: []metricdata.HistogramDataPoint[float64]{{
				Count: 4,
				Sum:   40,
				Min:   metricdata.NewExtrema[float64](2),
				Max:   metricdata.NewExtrema[float64](20),
				Time:  cwFixedTime,
			}},
		},
	}}}}}
	if err := exp.Export(context.Background(), rm); err != nil {
		t.Fatalf("Export: %v", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.calls) != 1 || len(f.calls[0].MetricData) != 1 {
		t.Fatalf("expected 1 datum, got %+v", f.calls)
	}
	d := f.calls[0].MetricData[0]
	if d.StatisticValues == nil {
		t.Fatalf("expected StatisticValues for histogram")
	}
	s := d.StatisticValues
	if *s.SampleCount != 4 || *s.Sum != 40 || *s.Minimum != 2 || *s.Maximum != 20 {
		t.Fatalf("unexpected statistic set: count=%v sum=%v min=%v max=%v", *s.SampleCount, *s.Sum, *s.Minimum, *s.Maximum)
	}
}

func TestCloudWatchMetricsFoldsAttributesAsDimensions(t *testing.T) {
	t.Parallel()
	f := &fakeCWMetrics{}
	exportMetric(t, f, "rss2msg", metricdata.Metrics{
		Name: "sink.publish.failures",
		Data: metricdata.Sum[int64]{
			DataPoints: []metricdata.DataPoint[int64]{{
				Value: 2,
				Time:  cwFixedTime,
				Attributes: attribute.NewSet(
					attribute.String("sink", "kafka"),
					attribute.String("reason", "timeout"),
				),
			}},
		},
	})
	data := f.allData()
	if len(data) != 1 {
		t.Fatalf("expected 1 datum, got %d", len(data))
	}
	// Dimensions are sorted by key for determinism.
	want := []string{"reason=timeout", "sink=kafka"}
	if len(data[0].dims) != 2 || data[0].dims[0] != want[0] || data[0].dims[1] != want[1] {
		t.Fatalf("unexpected dimensions: %v", data[0].dims)
	}
}

func TestCloudWatchMetricsCapsDimensionsAt30(t *testing.T) {
	t.Parallel()
	f := &fakeCWMetrics{}
	var kvs []attribute.KeyValue
	for i := 0; i < 35; i++ {
		// zero-padded keys so sort order is well-defined
		kvs = append(kvs, attribute.String(padKey(i), "v"))
	}
	exportMetric(t, f, "rss2msg", metricdata.Metrics{
		Name: "x",
		Data: metricdata.Sum[int64]{
			DataPoints: []metricdata.DataPoint[int64]{{Value: 1, Time: cwFixedTime, Attributes: attribute.NewSet(kvs...)}},
		},
	})
	data := f.allData()
	if len(data) != 1 {
		t.Fatalf("expected 1 datum, got %d", len(data))
	}
	if len(data[0].dims) != 30 {
		t.Fatalf("expected dimensions capped at 30, got %d", len(data[0].dims))
	}
}

func TestCloudWatchMetricsChunksAtThousand(t *testing.T) {
	t.Parallel()
	f := &fakeCWMetrics{}
	var dps []metricdata.DataPoint[int64]
	for i := 0; i < 2500; i++ {
		dps = append(dps, metricdata.DataPoint[int64]{Value: int64(i), Time: cwFixedTime})
	}
	exp := newCloudWatchMetricsExporter(f, "rss2msg")
	rm := &metricdata.ResourceMetrics{ScopeMetrics: []metricdata.ScopeMetrics{{Metrics: []metricdata.Metrics{{
		Name: "x",
		Data: metricdata.Sum[int64]{DataPoints: dps},
	}}}}}
	if err := exp.Export(context.Background(), rm); err != nil {
		t.Fatalf("Export: %v", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.calls) != 3 { // 1000 + 1000 + 500
		t.Fatalf("expected 3 chunked PutMetricData calls, got %d", len(f.calls))
	}
}

func TestCloudWatchMetricsExportAfterShutdownIsNoop(t *testing.T) {
	t.Parallel()
	f := &fakeCWMetrics{}
	exp := newCloudWatchMetricsExporter(f, "rss2msg")
	if err := exp.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	rm := &metricdata.ResourceMetrics{ScopeMetrics: []metricdata.ScopeMetrics{{Metrics: []metricdata.Metrics{{
		Name: "feed.fetches",
		Data: metricdata.Sum[int64]{DataPoints: []metricdata.DataPoint[int64]{{Value: 1, Time: cwFixedTime}}},
	}}}}}
	if err := exp.Export(context.Background(), rm); err != nil {
		t.Fatalf("Export after shutdown should be a noop, got %v", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.calls) != 0 {
		t.Fatalf("expected no PutMetricData calls after shutdown, got %d", len(f.calls))
	}
}

func TestSetupWiresCloudWatchMetricsWithoutNetwork(t *testing.T) {
	t.Parallel()
	cfg := config.Defaults()
	cfg.Telemetry.Metrics.Enabled = true
	cfg.Telemetry.CloudWatch.Enabled = true
	cfg.Telemetry.CloudWatch.Region = "us-east-1"
	cfg.Telemetry.CloudWatch.Metrics.Enabled = true
	// No instruments recorded, so the PeriodicReader's shutdown export is empty
	// and no PutMetricData call is made — Setup/Shutdown stay network-free.

	tel, err := Setup(context.Background(), cfg, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := tel.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

func padKey(i int) string {
	const digits = "0123456789"
	return "k" + string([]byte{digits[i/10], digits[i%10]})
}
