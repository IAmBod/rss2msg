package config

import (
	"strings"
	"testing"
)

func TestValidateRejectsGRPCSinkWithoutTarget(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Sinks = append(c.Sinks, SinkConfig{Name: "grpc-out", Driver: "grpc"})
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "grpc.target is required") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateRejectsGRPCSinkReservedMetadataKey(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Sinks = append(c.Sinks, SinkConfig{
		Name: "grpc-out", Driver: "grpc",
		GRPC: GRPCSinkConfig{Target: "127.0.0.1:50051", Metadata: map[string]string{"grpc-trace-bin": "x"}},
	})
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "reserved") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateRejectsGRPCSinkNegativeTimeout(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Sinks = append(c.Sinks, SinkConfig{
		Name: "grpc-out", Driver: "grpc",
		GRPC: GRPCSinkConfig{Target: "127.0.0.1:50051", Timeout: -1},
	})
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "timeout must not be negative") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateAcceptsGRPCSink(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Sinks = append(c.Sinks, SinkConfig{
		Name: "grpc-out", Driver: "grpc",
		GRPC: GRPCSinkConfig{
			Target:   "127.0.0.1:50051",
			Metadata: map[string]string{"authorization": "Bearer t"},
		},
	})
	if _, err := Validate(c); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestValidateRejectsGRPCSinkUnmatchedTLSKeyPair(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Sinks = append(c.Sinks, SinkConfig{
		Name: "grpc-out", Driver: "grpc",
		GRPC: GRPCSinkConfig{Target: "127.0.0.1:50051", TLS: SinkTLSConfig{CertFile: "c.pem"}},
	})
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "cert_file and key_file") {
		t.Fatalf("got %v", err)
	}
}
