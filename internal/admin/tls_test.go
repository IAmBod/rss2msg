package admin

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/iambod/rss2msg/internal/config"
	"github.com/iambod/rss2msg/internal/httpauth"
	"github.com/rs/zerolog"
)

// writeCertPair creates a self-signed cert usable as both a CA and a leaf for
// tests, returns the cert/key file paths and the parsed cert for pools.
func writeCertPair(t *testing.T, dir, name string) (certFile, keyFile string, cert *x509.Certificate, key *ecdsa.PrivateKey) {
	t.Helper()
	key, _ = ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(time.Now().UnixNano()),
		Subject:               pkix.Name{CommonName: name},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		IsCA:                  true,
		BasicConstraintsValid: true,
		DNSNames:              []string{"localhost"},
	}
	der, _ := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	cert, _ = x509.ParseCertificate(der)
	certFile = filepath.Join(dir, name+".crt")
	keyFile = filepath.Join(dir, name+".key")
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDER, _ := x509.MarshalECPrivateKey(key)
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	_ = os.WriteFile(certFile, certPEM, 0o600)
	_ = os.WriteFile(keyFile, keyPEM, 0o600)
	return
}

func TestMTLS(t *testing.T) {
	dir := t.TempDir()
	srvCert, srvKey, caCert, _ := writeCertPair(t, dir, "server")
	caFile := filepath.Join(dir, "ca.crt") // reuse server cert as CA (self-signed, IsCA)
	_ = os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caCert.Raw}), 0o600)
	cliCert, cliKey := srvCert, srvKey // client presents a cert signed by the same CA (self)

	cfg := config.AdminConfig{
		Enabled: true,
		Listen:  "127.0.0.1:0",
		TLS:     config.AdminTLSConfig{Enabled: true, CertFile: srvCert, KeyFile: srvKey, ClientCAFile: caFile},
	}
	s := New(cfg, &httpauth.Auth{}, baseDeps(), zerolog.Nop())
	if err := s.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = s.Shutdown(context.Background()) }()
	addr := s.server.Addr // set in Start via the bound listener (see impl note)

	clientCertPair, _ := tls.LoadX509KeyPair(cliCert, cliKey)
	caPool := x509.NewCertPool()
	caPool.AddCert(caCert)

	// with client cert => 200
	withCert := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{
		Certificates: []tls.Certificate{clientCertPair}, RootCAs: caPool, ServerName: "localhost",
	}}}
	resp, err := withCert.Get("https://" + addr + "/v1/status")
	if err != nil {
		t.Fatalf("mTLS client: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d want 200", resp.StatusCode)
	}

	// without client cert => handshake/transport error
	noCert := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: caPool, ServerName: "localhost"}}}
	if _, err := noCert.Get("https://" + addr + "/v1/status"); err == nil {
		t.Fatal("expected mTLS to reject a client with no cert")
	}
}
