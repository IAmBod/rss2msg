package telemetry

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// prometheusHTTPHandler returns an HTTP handler that serves metrics from the
// given gatherer. Pass the same prometheus.Registry that was provided to the
// OTEL prometheus exporter so that the /metrics endpoint actually exposes
// OTEL meters (and not just any incidental collectors on the default registry).
func prometheusHTTPHandler(g prometheus.Gatherer) http.Handler {
	return promhttp.HandlerFor(g, promhttp.HandlerOpts{})
}
