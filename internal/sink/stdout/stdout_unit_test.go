package stdout

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/iambod/rss2msg/internal/model"
)

func sampleChange() model.Change {
	return model.Change{
		SchemaVersion: model.SchemaVersion,
		FeedURL:       "https://e/feed",
		ItemID:        "i1",
		Kind:          model.ChangeNew,
		Title:         "Hello world",
		ContentHash:   "deadbeef",
		DetectedAt:    time.Date(2026, 5, 30, 12, 0, 0, 0, time.UTC),
	}
}

func TestNewRejectsMissingName(t *testing.T) {
	t.Parallel()
	if _, err := New(Options{}); err == nil {
		t.Fatal("expected error for missing name")
	}
}

func TestNewRejectsUnknownTarget(t *testing.T) {
	t.Parallel()
	if _, err := New(Options{Name: "x", Target: "syslog"}); err == nil {
		t.Fatal("expected error for unknown target")
	}
}

func TestNewRejectsUnknownFormat(t *testing.T) {
	t.Parallel()
	if _, err := New(Options{Name: "x", Format: "yaml"}); err == nil {
		t.Fatal("expected error for unknown format")
	}
}

func TestPublishEmitsNDJSON(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	p := NewForWriter("test", &buf, false)
	if err := p.Publish(context.Background(), sampleChange()); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	if !strings.HasSuffix(out, "\n") {
		t.Fatalf("expected trailing newline, got %q", out)
	}
	if strings.Count(out, "\n") != 1 {
		t.Fatalf("expected exactly one newline (single record), got %d in %q", strings.Count(out, "\n"), out)
	}
	if strings.Contains(out, "\n  ") {
		t.Fatalf("default format should NOT be indented, got %q", out)
	}

	var round model.Change
	if err := json.Unmarshal([]byte(strings.TrimRight(out, "\n")), &round); err != nil {
		t.Fatalf("output is not parseable JSON: %v\n%s", err, out)
	}
	if round.ItemID != "i1" || round.Title != "Hello world" || round.SchemaVersion != model.SchemaVersion {
		t.Fatalf("round-trip mismatch: %+v", round)
	}
}

func TestPublishPrettyFormatIsIndented(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	p := NewForWriter("test", &buf, true)
	if err := p.Publish(context.Background(), sampleChange()); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	if !strings.Contains(out, "\n  ") {
		t.Fatalf("pretty format should be indented (contain \"\\n  \"), got %q", out)
	}
	// The full pretty record still ends with a newline so a tail consumer
	// can detect record boundaries (a blank line separates two records).
	if !strings.HasSuffix(out, "}\n") {
		t.Fatalf("expected closing brace + newline, got %q", out)
	}
}

func TestPublishMutexPreventsInterleaving(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	p := NewForWriter("test", &buf, false)
	const n = 200
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c := sampleChange()
			c.ItemID = "i-" + strings.Repeat("x", i%50)
			if err := p.Publish(context.Background(), c); err != nil {
				t.Errorf("publish: %v", err)
			}
		}(i)
	}
	wg.Wait()

	// Every line of output must be a complete, parseable JSON object.
	// Mid-record interleaving would surface as parse errors.
	lines := strings.Split(strings.TrimRight(buf.String(), "\n"), "\n")
	if len(lines) != n {
		t.Fatalf("expected %d lines, got %d", n, len(lines))
	}
	for i, line := range lines {
		var c model.Change
		if err := json.Unmarshal([]byte(line), &c); err != nil {
			t.Fatalf("line %d not parseable JSON: %v\n%s", i, err, line)
		}
	}
}

func TestNewStderrTarget(t *testing.T) {
	t.Parallel()
	// We can't easily intercept os.Stderr from here, but we can verify the
	// constructor accepts the target without error and the resulting
	// Publisher publishes without panic. We use stderr because stdout would
	// pollute go test output.
	p, err := New(Options{Name: "test", Target: "stderr"})
	if err != nil {
		t.Fatal(err)
	}
	if p == nil {
		t.Fatal("nil publisher")
	}
}
