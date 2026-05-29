package rabbitmq

import "testing"

func TestNewRejectsMissingName(t *testing.T) {
	t.Parallel()
	if _, err := New(Options{URL: "amqp://localhost"}); err == nil {
		t.Fatal("expected error for missing name")
	}
}

func TestNewRejectsMissingURL(t *testing.T) {
	t.Parallel()
	if _, err := New(Options{Name: "x"}); err == nil {
		t.Fatal("expected error for missing url")
	}
}

func TestNewRejectsBadExchangeType(t *testing.T) {
	t.Parallel()
	if _, err := New(Options{Name: "x", URL: "amqp://localhost", ExchangeType: "broadcast"}); err == nil {
		t.Fatal("expected error for unknown exchange_type")
	}
}

func TestNewRejectsDeclareWithoutExchange(t *testing.T) {
	t.Parallel()
	if _, err := New(Options{Name: "x", URL: "amqp://localhost", Declare: true}); err == nil {
		t.Fatal("expected error for declare=true with empty exchange")
	}
}
