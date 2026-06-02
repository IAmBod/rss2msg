package nats

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
	certOut, err := os.Create(certPath)
	if err != nil {
		t.Fatalf("create cert file: %v", err)
	}
	if err := pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: der}); err != nil {
		t.Fatalf("encode cert: %v", err)
	}
	certOut.Close()
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	keyOut, err := os.Create(keyPath)
	if err != nil {
		t.Fatalf("create key file: %v", err)
	}
	if err := pem.Encode(keyOut, &pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}); err != nil {
		t.Fatalf("encode key: %v", err)
	}
	keyOut.Close()
	return certPath, keyPath
}

func TestBuildTLSConfig(t *testing.T) {
	dir := t.TempDir()
	certPath, keyPath := writeSelfSignedCert(t, dir)
	caPath := certPath // self-signed: the cert is its own CA

	t.Run("empty options leave ServerName empty", func(t *testing.T) {
		tc, err := buildTLSConfig(TLSOptions{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if tc.ServerName != "" {
			t.Errorf("ServerName = %q, want empty", tc.ServerName)
		}
		if tc.InsecureSkipVerify {
			t.Error("InsecureSkipVerify should be false")
		}
	})

	t.Run("CA file loads into RootCAs", func(t *testing.T) {
		tc, err := buildTLSConfig(TLSOptions{CAFile: caPath})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if tc.RootCAs == nil {
			t.Error("RootCAs should be set")
		}
	})

	t.Run("client cert loads", func(t *testing.T) {
		tc, err := buildTLSConfig(TLSOptions{CertFile: certPath, KeyFile: keyPath})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(tc.Certificates) != 1 {
			t.Errorf("Certificates len = %d, want 1", len(tc.Certificates))
		}
	})

	t.Run("server name override", func(t *testing.T) {
		tc, err := buildTLSConfig(TLSOptions{ServerName: "nats.example.com"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if tc.ServerName != "nats.example.com" {
			t.Errorf("ServerName = %q, want nats.example.com", tc.ServerName)
		}
	})

	t.Run("insecure skip verify", func(t *testing.T) {
		tc, err := buildTLSConfig(TLSOptions{InsecureSkipVerify: true})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !tc.InsecureSkipVerify {
			t.Error("InsecureSkipVerify should be true")
		}
	})

	t.Run("missing CA file errors", func(t *testing.T) {
		_, err := buildTLSConfig(TLSOptions{CAFile: "/nonexistent/ca.pem"})
		if err == nil {
			t.Error("expected error for missing CA file")
		}
	})

	t.Run("half cert pair errors", func(t *testing.T) {
		_, err := buildTLSConfig(TLSOptions{CertFile: certPath})
		if err == nil {
			t.Error("expected error for cert without key")
		}
	})
}
