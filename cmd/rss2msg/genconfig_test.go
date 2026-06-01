package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/iambod/rss2msg/internal/config"
)

func TestGenerateConfigCmd_RegisteredOnRoot(t *testing.T) {
	root := newRootCmd()
	for _, c := range root.Commands() {
		if c.Name() == "generate-config" {
			return
		}
	}
	t.Fatal("root command has no \"generate-config\" subcommand")
}

func TestGenerateConfigCmd_PrintsToStdout(t *testing.T) {
	cmd := newGenerateConfigCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(nil)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("generate-config failed: %v", err)
	}
	if !bytes.Equal(out.Bytes(), config.ExampleConfig()) {
		t.Fatalf("stdout output does not match config.ExampleConfig()")
	}
}

func TestGenerateConfigCmd_WritesOutputFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")

	cmd := newGenerateConfigCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"--output", path})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("generate-config --output failed: %v", err)
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read output file: %v", err)
	}
	if !bytes.Equal(got, config.ExampleConfig()) {
		t.Fatalf("written file does not match config.ExampleConfig()")
	}
	// Nothing should have gone to stdout when writing to a file.
	if out.Len() != 0 {
		t.Fatalf("expected no stdout when writing to file, got %q", out.String())
	}
}

func TestGenerateConfigCmd_RefusesToClobberWithoutForce(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte("# existing\n"), 0o600); err != nil {
		t.Fatalf("seed existing file: %v", err)
	}

	cmd := newGenerateConfigCmd()
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetArgs([]string{"-o", path})
	if err := cmd.Execute(); err == nil {
		t.Fatal("expected error when output file exists without --force")
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	if string(got) != "# existing\n" {
		t.Fatalf("existing file was modified without --force: %q", got)
	}
}

func TestGenerateConfigCmd_ForceOverwrites(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte("# existing\n"), 0o600); err != nil {
		t.Fatalf("seed existing file: %v", err)
	}

	cmd := newGenerateConfigCmd()
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetArgs([]string{"-o", path, "--force"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("generate-config -o --force failed: %v", err)
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	if !bytes.Equal(got, config.ExampleConfig()) {
		t.Fatalf("--force did not overwrite with example config")
	}
}
