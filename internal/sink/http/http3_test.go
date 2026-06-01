package http

import (
	"context"
	"crypto/tls"
	"net"
	stdhttp "net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/quic-go/quic-go/http3"

	"github.com/iambod/rss2msg/internal/model"
)

// startHTTP3Server spins an HTTP/3 test server on an ephemeral UDP port using a
// self-signed cert and returns its base URL ("https://host:port") plus a counter
// of received requests. It shuts down when the test finishes.
func startHTTP3Server(t *testing.T, h stdhttp.Handler) string {
	t.Helper()
	dir := t.TempDir()
	certPath, keyPath := writeSelfSignedCert(t, dir)
	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		t.Fatalf("load keypair: %v", err)
	}
	udpAddr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("resolve udp: %v", err)
	}
	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		t.Fatalf("listen udp: %v", err)
	}
	srv := &http3.Server{
		Handler:   h,
		TLSConfig: http3.ConfigureTLSConfig(&tls.Config{Certificates: []tls.Certificate{cert}}),
	}
	go func() { _ = srv.Serve(conn) }()
	t.Cleanup(func() { _ = srv.Close(); _ = conn.Close() })
	return "https://" + conn.LocalAddr().String()
}

func TestPublishOverHTTP3(t *testing.T) {
	var got atomic.Int32
	base := startHTTP3Server(t, stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		if r.ProtoMajor != 3 {
			t.Errorf("server saw proto major %d, want 3", r.ProtoMajor)
		}
		got.Add(1)
		w.WriteHeader(stdhttp.StatusOK)
	}))

	p, err := New(Options{
		Name:    "h3hook",
		URL:     base + "/hook",
		HTTP3:   true,
		Timeout: 5 * time.Second,
		TLS:     &TLSOptions{InsecureSkipVerify: true},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer p.Close()

	// The transport must be an HTTP/3 transport, not the stdlib one.
	if _, ok := p.client.Transport.(*http3.Transport); !ok {
		t.Fatalf("client.Transport = %T, want *http3.Transport", p.client.Transport)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := p.Publish(ctx, model.Change{FeedURL: "https://f", ItemID: "1", Kind: model.ChangeNew}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if got.Load() != 1 {
		t.Fatalf("server received %d requests, want 1", got.Load())
	}
}

func TestNewHTTP3RequiresHTTPS(t *testing.T) {
	_, err := New(Options{Name: "h3", URL: "http://example.com/hook", HTTP3: true})
	if err == nil {
		t.Fatal("expected error for http3 over plaintext http:// URL")
	}
}
