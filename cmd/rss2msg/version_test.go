package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestVersionCmd_PrintsBuildInfo(t *testing.T) {
	// Simulate values injected by GoReleaser's ldflags.
	defer restoreBuildInfo(version, commit, date)
	version, commit, date = "1.2.3", "abc1234", "2026-06-01T00:00:00Z"

	cmd := newVersionCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("version command failed: %v", err)
	}

	got := out.String()
	for _, want := range []string{"1.2.3", "abc1234", "2026-06-01T00:00:00Z"} {
		if !strings.Contains(got, want) {
			t.Errorf("version output missing %q\ngot: %s", want, got)
		}
	}
}

func TestVersionCmd_RegisteredOnRoot(t *testing.T) {
	root := newRootCmd()
	for _, c := range root.Commands() {
		if c.Name() == "version" {
			return
		}
	}
	t.Fatal("root command has no \"version\" subcommand")
}

func restoreBuildInfo(v, c, d string) {
	version, commit, date = v, c, d
}
