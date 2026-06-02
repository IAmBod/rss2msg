package feed

import "go.opentelemetry.io/otel/metric"

type instruments struct {
	requests    metric.Int64Counter
	notMod      metric.Int64Counter
	mcpRequests metric.Int64Counter
}

func newInstruments(m metric.Meter) (*instruments, error) {
	reqs, err := m.Int64Counter("feed_sink.requests")
	if err != nil {
		return nil, err
	}
	nm, err := m.Int64Counter("feed_sink.not_modified")
	if err != nil {
		return nil, err
	}
	mcpReqs, err := m.Int64Counter("feed_sink.mcp_requests")
	if err != nil {
		return nil, err
	}
	return &instruments{requests: reqs, notMod: nm, mcpRequests: mcpReqs}, nil
}
