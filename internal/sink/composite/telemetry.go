package composite

import "go.opentelemetry.io/otel/metric"

type instruments struct {
	publishes metric.Int64Counter // one per Publish call
	children  metric.Int64Counter // per child delivery, attr outcome=success|dlq|dropped
}

func newInstruments(m metric.Meter) (*instruments, error) {
	pubs, err := m.Int64Counter("composite_sink.publishes")
	if err != nil {
		return nil, err
	}
	ch, err := m.Int64Counter("composite_sink.child_deliveries")
	if err != nil {
		return nil, err
	}
	return &instruments{publishes: pubs, children: ch}, nil
}
