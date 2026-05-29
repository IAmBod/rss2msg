package redis

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

// writeSelfSignedCert generates a self-signed ECDSA cert/key pair and writes
// them as PEM files under dir. Returns the cert and key paths. Suitable for
// exercising TLS-config-builder logic without needing a real TLS handshake.
func writeSelfSignedCert(t *testing.T, dir, cn string) (certPath, keyPath string) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:     []string{cn},
		IsCA:         true,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &priv.PublicKey, priv)
	if err != nil {
		t.Fatal(err)
	}
	certPath = filepath.Join(dir, "cert.pem")
	keyPath = filepath.Join(dir, "key.pem")
	if err := os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	keyBytes, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyBytes}), 0o600); err != nil {
		t.Fatal(err)
	}
	return certPath, keyPath
}

func TestBuildTLSConfigDefaultsServerName(t *testing.T) {
	t.Parallel()
	tc, err := buildTLSConfig(TLSOptions{}, "redis.internal")
	if err != nil {
		t.Fatal(err)
	}
	if tc.ServerName != "redis.internal" {
		t.Fatalf("expected default server name from URL, got %q", tc.ServerName)
	}
	if tc.InsecureSkipVerify {
		t.Fatal("expected InsecureSkipVerify=false by default")
	}
	if tc.RootCAs != nil {
		t.Fatal("expected system roots (RootCAs nil) by default")
	}
	if len(tc.Certificates) != 0 {
		t.Fatal("expected no client certs by default")
	}
}

func TestBuildTLSConfigServerNameOverride(t *testing.T) {
	t.Parallel()
	tc, err := buildTLSConfig(TLSOptions{ServerName: "override.internal"}, "redis.internal")
	if err != nil {
		t.Fatal(err)
	}
	if tc.ServerName != "override.internal" {
		t.Fatalf("ServerName override ignored, got %q", tc.ServerName)
	}
}

func TestBuildTLSConfigInsecureSkipVerify(t *testing.T) {
	t.Parallel()
	tc, err := buildTLSConfig(TLSOptions{InsecureSkipVerify: true}, "redis.internal")
	if err != nil {
		t.Fatal(err)
	}
	if !tc.InsecureSkipVerify {
		t.Fatal("InsecureSkipVerify not propagated")
	}
}

func TestBuildTLSConfigLoadsCA(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	certPath, _ := writeSelfSignedCert(t, dir, "ca.test")
	tc, err := buildTLSConfig(TLSOptions{CAFile: certPath}, "redis.internal")
	if err != nil {
		t.Fatal(err)
	}
	if tc.RootCAs == nil {
		t.Fatal("expected RootCAs populated from ca_file")
	}
}

func TestBuildTLSConfigRejectsBadCAFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	bad := filepath.Join(dir, "not-a-cert.pem")
	if err := os.WriteFile(bad, []byte("this is not a PEM cert"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := buildTLSConfig(TLSOptions{CAFile: bad}, "redis.internal")
	if err == nil {
		t.Fatal("expected error for malformed CA file")
	}
}

func TestBuildTLSConfigRejectsMissingCAFile(t *testing.T) {
	t.Parallel()
	_, err := buildTLSConfig(TLSOptions{CAFile: "/nonexistent/ca.pem"}, "redis.internal")
	if err == nil {
		t.Fatal("expected error for missing CA file")
	}
}

func TestBuildTLSConfigLoadsClientCert(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	certPath, keyPath := writeSelfSignedCert(t, dir, "client.test")
	tc, err := buildTLSConfig(TLSOptions{CertFile: certPath, KeyFile: keyPath}, "redis.internal")
	if err != nil {
		t.Fatal(err)
	}
	if len(tc.Certificates) != 1 {
		t.Fatalf("expected exactly one client cert, got %d", len(tc.Certificates))
	}
}

func TestBuildTLSConfigRejectsOnlyCertFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	certPath, _ := writeSelfSignedCert(t, dir, "client.test")
	_, err := buildTLSConfig(TLSOptions{CertFile: certPath}, "redis.internal")
	if err == nil {
		t.Fatal("expected error when key_file is missing")
	}
}
