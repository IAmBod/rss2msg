package config

import (
	"os"
	"path/filepath"
	"testing"
)

// TestExampleConfigRoundTrips proves the embedded annotated reference is a
// genuinely valid, runnable config: it must Load and Validate without error.
func TestExampleConfigRoundTrips(t *testing.T) {
	data := ExampleConfig()
	if len(data) == 0 {
		t.Fatal("ExampleConfig() returned no bytes")
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write temp config: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load embedded example: %v", err)
	}
	warnings, err := Validate(cfg)
	if err != nil {
		t.Fatalf("Validate embedded example: %v (warnings: %v)", err, warnings)
	}
}

// TestExampleConfigMatchesRepoRoot is the drift guard: the embedded asset must
// be byte-identical to the maintained reference at examples/config.example.yaml,
// so editing one without the other fails CI.
func TestExampleConfigMatchesRepoRoot(t *testing.T) {
	// Test working dir is the package dir (internal/config); the repo root is
	// two levels up.
	root := filepath.Join("..", "..", "examples", "config.example.yaml")
	want, err := os.ReadFile(root)
	if err != nil {
		t.Fatalf("read %s: %v", root, err)
	}
	if got := ExampleConfig(); string(got) != string(want) {
		t.Fatalf("embedded example.yaml has drifted from %s; copy it back:\n\tcp examples/config.example.yaml internal/config/example.yaml", root)
	}
}
