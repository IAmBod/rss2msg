package feed

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	stdhttp "net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/quic-go/quic-go/http3"

	"github.com/iambod/rss2msg/internal/model"
)

// writeFeedSelfSignedCert writes a self-signed cert+key (valid for 127.0.0.1)
// and returns their file paths.
func writeFeedSelfSignedCert(t *testing.T, dir string) (certPath, keyPath string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "127.0.0.1"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1)},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create cert: %v", err)
	}
	certPath = filepath.Join(dir, "cert.pem")
	keyPath = filepath.Join(dir, "key.pem")
	certOut, _ := os.Create(certPath)
	if err := pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: der}); err != nil {
		t.Fatalf("encode cert: %v", err)
	}
	certOut.Close()
	keyDER, _ := x509.MarshalECPrivateKey(key)
	keyOut, _ := os.Create(keyPath)
	if err := pem.Encode(keyOut, &pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}); err != nil {
		t.Fatalf("encode key: %v", err)
	}
	keyOut.Close()
	return certPath, keyPath
}

func TestFeedSinkServesOverHTTP3(t *testing.T) {
	dir := t.TempDir()
	certPath, keyPath := writeFeedSelfSignedCert(t, dir)

	ctx := context.Background()
	p, err := New(ctx, Options{
		Name:        "h3feed",
		Listen:      "127.0.0.1:0",
		Meta:        FeedMeta{Title: "t", Link: "https://example.com/"},
		MaxItems:    10,
		TLSCertFile: certPath,
		TLSKeyFile:  keyPath,
		HTTP3:       true,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer p.Close()

	if err := p.Publish(ctx, model.Change{FeedURL: "https://f", ItemID: "1", Kind: model.ChangeNew, Title: "hello"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	addr := p.Addr() // 127.0.0.1:<port>, shared by TCP and UDP

	// HTTP/3 client (QUIC over UDP).
	tr := &http3.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}} //nolint:gosec // test
	defer tr.Close()
	client := &stdhttp.Client{Transport: tr, Timeout: 5 * time.Second}

	resp, err := client.Get("https://" + addr + "/rss")
	if err != nil {
		t.Fatalf("http3 GET /rss: %v", err)
	}
	defer resp.Body.Close()
	if resp.ProtoMajor != 3 {
		t.Errorf("response proto major = %d, want 3", resp.ProtoMajor)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "hello") {
		t.Errorf("rss body missing published item; got: %s", body)
	}

	// The TCP (H1/H2) responses must advertise HTTP/3 via Alt-Svc so clients
	// can discover the upgrade.
	tcpClient := &stdhttp.Client{
		Transport: &stdhttp.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}, //nolint:gosec // test
		Timeout:   5 * time.Second,
	}
	u := url.URL{Scheme: "https", Host: addr, Path: "/rss"}
	tcpResp, err := tcpClient.Get(u.String())
	if err != nil {
		t.Fatalf("tcp GET /rss: %v", err)
	}
	defer tcpResp.Body.Close()
	if alt := tcpResp.Header.Get("Alt-Svc"); !strings.Contains(alt, "h3") {
		t.Errorf("Alt-Svc header missing h3 advertisement; got %q", alt)
	}
}

func TestFeedSinkHTTP3RequiresTLS(t *testing.T) {
	_, err := New(context.Background(), Options{
		Name:     "h3feed",
		Listen:   "127.0.0.1:0",
		Meta:     FeedMeta{Title: "t", Link: "https://example.com/"},
		MaxItems: 10,
		HTTP3:    true,
		// no cert/key
	})
	if err == nil {
		t.Fatal("expected error for http3 without TLS cert/key")
	}
}
