package feedsource

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// writeSelfSignedCert writes a PEM cert+key pair to dir and returns their paths.
// Mirrors the helper used by the sink/coordinator TLS unit tests.
func writeSelfSignedCert(t *testing.T, dir string) (certPath, keyPath string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create cert: %v", err)
	}
	certPath = filepath.Join(dir, "cert.pem")
	keyPath = filepath.Join(dir, "key.pem")
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	if err := os.WriteFile(certPath, certPEM, 0o600); err != nil {
		t.Fatalf("write cert: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	if err := os.WriteFile(keyPath, keyPEM, 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}
	return certPath, keyPath
}

func TestBuildPostgresTLSDefaults(t *testing.T) {
	t.Parallel()
	tc, err := buildPostgresTLS(PostgresTLSOptions{}, "db.internal")
	if err != nil {
		t.Fatalf("buildPostgresTLS: %v", err)
	}
	if tc.ServerName != "db.internal" {
		t.Fatalf("ServerName = %q, want the DSN-derived default %q", tc.ServerName, "db.internal")
	}
	if tc.InsecureSkipVerify {
		t.Fatalf("InsecureSkipVerify should default to false")
	}
	if tc.RootCAs != nil || len(tc.Certificates) != 0 {
		t.Fatalf("no CA/client cert configured, but RootCAs/Certificates were set")
	}
}

func TestBuildPostgresTLSServerNameOverrideAndInsecure(t *testing.T) {
	t.Parallel()
	tc, err := buildPostgresTLS(PostgresTLSOptions{
		ServerName:         "override.example",
		InsecureSkipVerify: true,
	}, "db.internal")
	if err != nil {
		t.Fatalf("buildPostgresTLS: %v", err)
	}
	if tc.ServerName != "override.example" {
		t.Fatalf("ServerName = %q, want the explicit override", tc.ServerName)
	}
	if !tc.InsecureSkipVerify {
		t.Fatalf("InsecureSkipVerify = false, want true")
	}
}

func TestBuildPostgresTLSWithCAFile(t *testing.T) {
	t.Parallel()
	caPath, _ := writeSelfSignedCert(t, t.TempDir())
	tc, err := buildPostgresTLS(PostgresTLSOptions{CAFile: caPath}, "db.internal")
	if err != nil {
		t.Fatalf("buildPostgresTLS: %v", err)
	}
	if tc.RootCAs == nil {
		t.Fatalf("RootCAs not set from a valid CA file")
	}
}

func TestBuildPostgresTLSWithClientCert(t *testing.T) {
	t.Parallel()
	certPath, keyPath := writeSelfSignedCert(t, t.TempDir())
	tc, err := buildPostgresTLS(PostgresTLSOptions{CertFile: certPath, KeyFile: keyPath}, "db.internal")
	if err != nil {
		t.Fatalf("buildPostgresTLS: %v", err)
	}
	if len(tc.Certificates) != 1 {
		t.Fatalf("Certificates = %d, want 1 client cert", len(tc.Certificates))
	}
}

func TestBuildPostgresTLSErrors(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	certPath, keyPath := writeSelfSignedCert(t, dir)

	bogusPEM := filepath.Join(dir, "bogus.pem")
	if err := os.WriteFile(bogusPEM, []byte("not a pem"), 0o600); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name string
		opts PostgresTLSOptions
	}{
		{"missing ca file", PostgresTLSOptions{CAFile: filepath.Join(dir, "nope.pem")}},
		{"invalid ca pem", PostgresTLSOptions{CAFile: bogusPEM}},
		{"cert without key", PostgresTLSOptions{CertFile: certPath}},
		{"key without cert", PostgresTLSOptions{KeyFile: keyPath}},
		{"unloadable client cert", PostgresTLSOptions{
			CertFile: filepath.Join(dir, "missing-cert.pem"),
			KeyFile:  filepath.Join(dir, "missing-key.pem"),
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if _, err := buildPostgresTLS(tc.opts, "db.internal"); err == nil {
				t.Fatalf("expected an error for %s, got nil", tc.name)
			}
		})
	}
}
