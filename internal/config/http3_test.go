package config

import (
	"strings"
	"testing"
)

func TestValidateRejectsHTTPSinkHTTP3OverPlaintext(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Sinks = append(c.Sinks, SinkConfig{
		Name: "h3-hook", Driver: "http",
		HTTP: HTTPSinkConfig{URL: "http://example.com/hook", HTTP3: true},
	})
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "http3 requires an https") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateAcceptsHTTPSinkHTTP3OverHTTPS(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Sinks = append(c.Sinks, SinkConfig{
		Name: "h3-hook", Driver: "http",
		HTTP: HTTPSinkConfig{URL: "https://example.com/hook", HTTP3: true},
	})
	if _, err := Validate(c); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestValidateRejectsFeedSinkHTTP3WithoutTLS(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Sinks = append(c.Sinks, SinkConfig{
		Name: "h3-feed", Driver: "feed",
		Feed: FeedSinkConfig{Listen: ":8088", HTTP3: true},
	})
	_, err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "http3 requires tls.cert_file and key_file") {
		t.Fatalf("got %v", err)
	}
}

func TestValidateAcceptsFeedSinkHTTP3WithTLS(t *testing.T) {
	t.Parallel()
	c := goodCfg()
	c.Sinks = append(c.Sinks, SinkConfig{
		Name: "h3-feed", Driver: "feed",
		Feed: FeedSinkConfig{Listen: ":8088", HTTP3: true, TLS: FeedTLSConfig{CertFile: "/c.pem", KeyFile: "/k.pem"}},
	})
	if _, err := Validate(c); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}
