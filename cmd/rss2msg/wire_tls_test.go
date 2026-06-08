package main

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/iambod/rss2msg/internal/config"
)

// The *TLSFromConfig helpers each map a config TLS block to a driver-specific
// *TLSOptions, returning nil when the block is inactive so the driver keeps its
// default (DSN-derived / system-roots) TLS behaviour. They are pure functions
// that gate TLS, so getting the active/inactive boundary and the field mapping
// right is security-relevant. Every output type exposes the same five fields,
// so the assertions read them by name via reflection.

// tlsFields is the common shape shared by every driver's *TLSOptions.
type tlsFields struct {
	ca, cert, key, server string
	insecure              bool
}

// wantMapped is the populated field set the "active" cases below feed in and
// expect mapped through verbatim.
var wantMapped = tlsFields{ca: "ca.pem", cert: "client.pem", key: "client.key", server: "sni.host", insecure: true}

// readTLSFields pulls the five canonical fields out of any *TLSOptions value.
func readTLSFields(t *testing.T, v any) tlsFields {
	t.Helper()
	rv := reflect.ValueOf(v)
	require.Equal(t, reflect.Ptr, rv.Kind())
	require.False(t, rv.IsNil(), "active TLS block must map to a non-nil options pointer")
	e := rv.Elem()
	return tlsFields{
		ca:       e.FieldByName("CAFile").String(),
		cert:     e.FieldByName("CertFile").String(),
		key:      e.FieldByName("KeyFile").String(),
		server:   e.FieldByName("ServerName").String(),
		insecure: e.FieldByName("InsecureSkipVerify").Bool(),
	}
}

func isNilPtr(v any) bool {
	rv := reflect.ValueOf(v)
	return rv.Kind() == reflect.Ptr && rv.IsNil()
}

// A fully-populated SinkTLSConfig and the matching populated blocks for the
// state / coordinator variants (which have no Enabled field).
var (
	activeSinkTLS = config.SinkTLSConfig{
		CAFile: "ca.pem", CertFile: "client.pem", KeyFile: "client.key",
		ServerName: "sni.host", InsecureSkipVerify: true,
	}
	activeStatePG = config.StatePGTLSConfig{
		CAFile: "ca.pem", CertFile: "client.pem", KeyFile: "client.key",
		ServerName: "sni.host", InsecureSkipVerify: true,
	}
	activeCoordPG = config.CoordinationPGTLSConfig{
		CAFile: "ca.pem", CertFile: "client.pem", KeyFile: "client.key",
		ServerName: "sni.host", InsecureSkipVerify: true,
	}
	activeRedis = config.CoordinationRedisTLSConfig{
		CAFile: "ca.pem", CertFile: "client.pem", KeyFile: "client.key",
		ServerName: "sni.host", InsecureSkipVerify: true,
	}
)

func TestTLSFromConfigMapping(t *testing.T) {
	cases := []struct {
		name     string
		inactive any // result of the mapper on a zero (inactive) block
		active   any // result of the mapper on a fully-populated block
	}{
		{"statePG", statePGTLSFromConfig(config.StatePGTLSConfig{}), statePGTLSFromConfig(activeStatePG)},
		{"sinkPG", sinkPGTLSFromConfig(config.SinkTLSConfig{}), sinkPGTLSFromConfig(activeSinkTLS)},
		{"sinkKafka", sinkKafkaTLSFromConfig(config.SinkTLSConfig{}), sinkKafkaTLSFromConfig(activeSinkTLS)},
		{"sinkRabbitMQ", sinkRabbitMQTLSFromConfig(config.SinkTLSConfig{}), sinkRabbitMQTLSFromConfig(activeSinkTLS)},
		{"sinkNATS", sinkNATSTLSFromConfig(config.SinkTLSConfig{}), sinkNATSTLSFromConfig(activeSinkTLS)},
		{"sinkHTTP", sinkHTTPTLSFromConfig(config.SinkTLSConfig{}), sinkHTTPTLSFromConfig(activeSinkTLS)},
		{"sinkGRPC", sinkGRPCTLSFromConfig(config.SinkTLSConfig{}), sinkGRPCTLSFromConfig(activeSinkTLS)},
		{"coordPG", coordPGTLSFromConfig(config.CoordinationPGTLSConfig{}), coordPGTLSFromConfig(activeCoordPG)},
		{"redis", redisTLSFromConfig(config.CoordinationRedisTLSConfig{}), redisTLSFromConfig(activeRedis)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.True(t, isNilPtr(tc.inactive), "an inactive TLS block must map to nil")
			require.Equal(t, wantMapped, readTLSFields(t, tc.active))
		})
	}
}

// A SinkTLSConfig with only Enabled set is active (force TLS with system roots)
// even though no files are given: it must map to a non-nil options with empty
// file fields, not nil.
func TestSinkTLSEnabledOnlyIsActive(t *testing.T) {
	got := sinkKafkaTLSFromConfig(config.SinkTLSConfig{Enabled: true})
	require.Equal(t, tlsFields{}, readTLSFields(t, got),
		"enabled-only block maps to non-nil options with empty fields")
}

// A single non-default field is enough to activate the file-gated (state /
// coordinator) variants, which have no Enabled flag.
func TestFileGatedTLSActivatesOnAnySingleField(t *testing.T) {
	require.False(t, isNilPtr(statePGTLSFromConfig(config.StatePGTLSConfig{InsecureSkipVerify: true})))
	require.False(t, isNilPtr(coordPGTLSFromConfig(config.CoordinationPGTLSConfig{CAFile: "ca.pem"})))
	require.False(t, isNilPtr(redisTLSFromConfig(config.CoordinationRedisTLSConfig{ServerName: "h"})))
}
