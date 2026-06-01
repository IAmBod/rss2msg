package telemetry

import (
	"context"
	"sort"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/iambod/rss2msg/internal/config"
)

const (
	// maxMetricData is the most MetricDatum entries CloudWatch accepts in one
	// PutMetricData call.
	maxMetricData = 1000
	// maxDimensions is CloudWatch's per-metric dimension limit.
	maxDimensions = 30
	// defaultMetricNamespace is used when none is configured.
	defaultMetricNamespace = "rss2msg"
)

// cloudWatchMetricsAPI is the subset of the CloudWatch client the exporter
// depends on, narrowed so a fake can satisfy it without contacting AWS.
type cloudWatchMetricsAPI interface {
	PutMetricData(context.Context, *cloudwatch.PutMetricDataInput, ...func(*cloudwatch.Options)) (*cloudwatch.PutMetricDataOutput, error)
}

// cloudWatchMetricsExporter is an OTEL metric Exporter that pushes data points
// to CloudWatch via PutMetricData. Cumulative temporality is used (matching the
// SDK default and the other push exporters). Each Export converts the resource
// metrics into MetricDatum entries, folds attributes into Dimensions, and sends
// them in PutMetricData-sized chunks.
type cloudWatchMetricsExporter struct {
	api       cloudWatchMetricsAPI
	namespace string

	mu       sync.Mutex
	shutdown bool
}

func newCloudWatchMetricsExporter(api cloudWatchMetricsAPI, namespace string) *cloudWatchMetricsExporter {
	if namespace == "" {
		namespace = defaultMetricNamespace
	}
	return &cloudWatchMetricsExporter{api: api, namespace: namespace}
}

// Temporality reports the temporality for an instrument kind. Cumulative is the
// SDK default and the right fit for CloudWatch.
func (e *cloudWatchMetricsExporter) Temporality(k sdkmetric.InstrumentKind) metricdata.Temporality {
	return sdkmetric.DefaultTemporalitySelector(k)
}

// Aggregation reports the aggregation for an instrument kind.
func (e *cloudWatchMetricsExporter) Aggregation(k sdkmetric.InstrumentKind) sdkmetric.Aggregation {
	return sdkmetric.DefaultAggregationSelector(k)
}

// Export converts rm to MetricDatum entries and pushes them to CloudWatch.
func (e *cloudWatchMetricsExporter) Export(ctx context.Context, rm *metricdata.ResourceMetrics) error {
	e.mu.Lock()
	down := e.shutdown
	e.mu.Unlock()
	if down {
		return nil
	}

	var data []cwtypes.MetricDatum
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			data = appendMetricData(data, m)
		}
	}
	if len(data) == 0 {
		return nil
	}

	for start := 0; start < len(data); start += maxMetricData {
		end := start + maxMetricData
		if end > len(data) {
			end = len(data)
		}
		if _, err := e.api.PutMetricData(ctx, &cloudwatch.PutMetricDataInput{
			Namespace:  aws.String(e.namespace),
			MetricData: data[start:end],
		}); err != nil {
			return err
		}
	}
	return nil
}

// ForceFlush is a no-op: Export pushes synchronously and holds no buffer.
func (e *cloudWatchMetricsExporter) ForceFlush(context.Context) error { return nil }

// Shutdown marks the exporter stopped; subsequent Export calls are no-ops.
func (e *cloudWatchMetricsExporter) Shutdown(context.Context) error {
	e.mu.Lock()
	e.shutdown = true
	e.mu.Unlock()
	return nil
}

func appendMetricData(data []cwtypes.MetricDatum, m metricdata.Metrics) []cwtypes.MetricDatum {
	switch d := m.Data.(type) {
	case metricdata.Sum[int64]:
		return appendNum(data, m.Name, d.DataPoints)
	case metricdata.Sum[float64]:
		return appendNum(data, m.Name, d.DataPoints)
	case metricdata.Gauge[int64]:
		return appendNum(data, m.Name, d.DataPoints)
	case metricdata.Gauge[float64]:
		return appendNum(data, m.Name, d.DataPoints)
	case metricdata.Histogram[int64]:
		return appendHist(data, m.Name, d.DataPoints)
	case metricdata.Histogram[float64]:
		return appendHist(data, m.Name, d.DataPoints)
	}
	return data
}

func appendNum[N int64 | float64](data []cwtypes.MetricDatum, name string, dps []metricdata.DataPoint[N]) []cwtypes.MetricDatum {
	for _, dp := range dps {
		ts := dp.Time
		data = append(data, cwtypes.MetricDatum{
			MetricName: aws.String(name),
			Value:      aws.Float64(float64(dp.Value)),
			Timestamp:  &ts,
			Dimensions: toDimensions(dp.Attributes),
		})
	}
	return data
}

func appendHist[N int64 | float64](data []cwtypes.MetricDatum, name string, dps []metricdata.HistogramDataPoint[N]) []cwtypes.MetricDatum {
	for _, dp := range dps {
		stat := cwtypes.StatisticSet{
			SampleCount: aws.Float64(float64(dp.Count)),
			Sum:         aws.Float64(float64(dp.Sum)),
			Minimum:     aws.Float64(0),
			Maximum:     aws.Float64(0),
		}
		if v, ok := dp.Min.Value(); ok {
			stat.Minimum = aws.Float64(float64(v))
		}
		if v, ok := dp.Max.Value(); ok {
			stat.Maximum = aws.Float64(float64(v))
		}
		ts := dp.Time
		data = append(data, cwtypes.MetricDatum{
			MetricName:      aws.String(name),
			StatisticValues: &stat,
			Timestamp:       &ts,
			Dimensions:      toDimensions(dp.Attributes),
		})
	}
	return data
}

// toDimensions folds an attribute set into CloudWatch Dimensions, sorted by key
// for deterministic output and capped at the 30-dimension limit.
func toDimensions(attrs attribute.Set) []cwtypes.Dimension {
	if attrs.Len() == 0 {
		return nil
	}
	type kv struct{ k, v string }
	pairs := make([]kv, 0, attrs.Len())
	for it := attrs.Iter(); it.Next(); {
		a := it.Attribute()
		pairs = append(pairs, kv{k: string(a.Key), v: a.Value.String()})
	}
	sort.Slice(pairs, func(i, j int) bool { return pairs[i].k < pairs[j].k })
	if len(pairs) > maxDimensions {
		pairs = pairs[:maxDimensions]
	}
	dims := make([]cwtypes.Dimension, 0, len(pairs))
	for _, p := range pairs {
		dims = append(dims, cwtypes.Dimension{Name: aws.String(p.k), Value: aws.String(p.v)})
	}
	return dims
}

// setupCloudWatchMetrics builds a CloudWatch client from cfg and returns a
// PeriodicReader option wrapping the exporter. The push cadence is the
// configured interval (0 uses the SDK default).
func setupCloudWatchMetrics(ctx context.Context, cfg config.TelemetryCloudWatchConfig) (sdkmetric.Option, error) {
	awsCfg, err := loadCloudWatchAWSConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	var clientOpts []func(*cloudwatch.Options)
	if cfg.EndpointURL != "" {
		clientOpts = append(clientOpts, func(o *cloudwatch.Options) {
			o.BaseEndpoint = aws.String(cfg.EndpointURL)
		})
	}
	client := cloudwatch.NewFromConfig(awsCfg, clientOpts...)

	exp := newCloudWatchMetricsExporter(client, cfg.Metrics.Namespace)

	var ropts []sdkmetric.PeriodicReaderOption
	if cfg.Metrics.Interval > 0 {
		ropts = append(ropts, sdkmetric.WithInterval(cfg.Metrics.Interval))
	}
	return sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exp, ropts...)), nil
}
