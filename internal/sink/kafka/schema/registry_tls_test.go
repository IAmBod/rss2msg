package schema

import (
	"os"
	"path/filepath"
	"testing"
)

func TestBuildTLSConfigCertKeyPairing(t *testing.T) {
	if _, err := buildTLSConfig(TLSOptions{CertFile: "x"}); err == nil {
		t.Fatal("expected error when only cert_file set")
	}
}

func TestBuildTLSConfigBadCAFile(t *testing.T) {
	dir := t.TempDir()
	bad := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(bad, []byte("not a pem"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := buildTLSConfig(TLSOptions{CAFile: bad}); err == nil {
		t.Fatal("expected error for non-PEM ca file")
	}
}

func TestBuildTLSConfigServerNameAndInsecure(t *testing.T) {
	tc, err := buildTLSConfig(TLSOptions{ServerName: "sr.local", InsecureSkipVerify: true})
	if err != nil {
		t.Fatal(err)
	}
	if tc.ServerName != "sr.local" || !tc.InsecureSkipVerify {
		t.Fatalf("bad tls config: %+v", tc)
	}
}
