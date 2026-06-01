package config

import (
	"strings"
	"testing"
)

func TestSinkTLSConfig_Active(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		tls  SinkTLSConfig
		want bool
	}{
		{"empty is inactive", SinkTLSConfig{}, false},
		{"enabled", SinkTLSConfig{Enabled: true}, true},
		{"ca_file", SinkTLSConfig{CAFile: "/ca.pem"}, true},
		{"cert_file", SinkTLSConfig{CertFile: "/c.pem"}, true},
		{"key_file", SinkTLSConfig{KeyFile: "/k.pem"}, true},
		{"server_name", SinkTLSConfig{ServerName: "host"}, true},
		{"insecure_skip_verify", SinkTLSConfig{InsecureSkipVerify: true}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.tls.Active(); got != tt.want {
				t.Errorf("Active() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestValidateRejectsPostgresSinkHalfCertPair(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Sinks = append(c.Sinks, SinkConfig{
		Name: "pg-tls", Driver: "postgres",
		Postgres: PostgresSinkConfig{DSN: "postgres://x", Table: "t", TLS: SinkTLSConfig{CertFile: "/c.pem"}},
	})
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "cert_file and key_file must both be set or both empty") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateRejectsHTTPSinkHalfKeyPair(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Sinks = append(c.Sinks, SinkConfig{
		Name: "hook-tls", Driver: "http",
		HTTP: HTTPSinkConfig{URL: "https://example.com", TLS: SinkTLSConfig{KeyFile: "/k.pem"}},
	})
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "cert_file and key_file must both be set or both empty") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateAcceptsKafkaSinkTLSEnabled(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Sinks = append(c.Sinks, SinkConfig{
		Name: "kafka-tls", Driver: "kafka",
		Kafka: KafkaSinkConfig{Brokers: []string{"b:9093"}, Topic: "t", TLS: SinkTLSConfig{Enabled: true, CAFile: "/ca.pem"}},
	})
	if _, err := Validate(c); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}
