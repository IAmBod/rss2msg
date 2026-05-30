package feedsource

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writeFeeds(t *testing.T, path, body string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestFileSourceReadsFeeds(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "feeds.json")
	writeFeeds(t, path, `[{"url":"https://e/1","interval":"5m"}]`)

	s, err := NewFile("file", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })

	got, err := s.Feeds(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].URL != "https://e/1" || got[0].Interval != 5*time.Minute {
		t.Fatalf("feeds = %+v", got)
	}
}

func TestFileSourceSignalsOnChange(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "feeds.json")
	writeFeeds(t, path, `[{"url":"https://e/1"}]`)

	s, err := NewFile("file", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })

	writeFeeds(t, path, `[{"url":"https://e/2"}]`)
	select {
	case <-s.Changes():
	case <-time.After(2 * time.Second):
		t.Fatal("expected a change signal after rewrite")
	}
}

func TestFileSourceRelativePathSignals(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	writeFeeds(t, "feeds.json", `[{"url":"https://e/1"}]`)

	s, err := NewFile("file", "feeds.json")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })

	writeFeeds(t, "feeds.json", `[{"url":"https://e/2"}]`)
	select {
	case <-s.Changes():
	case <-time.After(2 * time.Second):
		t.Fatal("expected a change signal for a relative path")
	}
}

func TestFileSourceMissingFileIsEmptyNotError(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "absent.json")
	s, err := NewFile("file", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })

	got, err := s.Feeds(context.Background())
	if err != nil {
		t.Fatalf("missing file should be empty, not error: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("want empty, got %+v", got)
	}
}
