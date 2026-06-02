package feedsource

import (
	"context"
	"testing"
)

func TestSpecFromRow(t *testing.T) {
	tests := []struct {
		name     string
		row      map[string]any
		wantURL  string
		wantInt  string
		wantSink []string
		wantErr  bool
	}{
		{
			name:    "url only",
			row:     map[string]any{"url": "https://a.example/feed"},
			wantURL: "https://a.example/feed",
		},
		{
			name:     "all fields",
			row:      map[string]any{"url": "https://b.example/feed", "interval": "15m", "sinks": []string{"x", "y"}},
			wantURL:  "https://b.example/feed",
			wantInt:  "15m",
			wantSink: []string{"x", "y"},
		},
		{
			name:     "sinks as []interface{} (text[] via RowToMap)",
			row:      map[string]any{"url": "https://c.example/feed", "sinks": []interface{}{"a", "b"}},
			wantURL:  "https://c.example/feed",
			wantSink: []string{"a", "b"},
		},
		{
			name:     "sinks as JSON array string",
			row:      map[string]any{"url": "https://d.example/feed", "sinks": `["one","two"]`},
			wantURL:  "https://d.example/feed",
			wantSink: []string{"one", "two"},
		},
		{
			name:     "sinks as single bare string",
			row:      map[string]any{"url": "https://e.example/feed", "sinks": "solo"},
			wantURL:  "https://e.example/feed",
			wantSink: []string{"solo"},
		},
		{
			name:    "interval as []byte (text column)",
			row:     map[string]any{"url": "https://f.example/feed", "interval": []byte("30s")},
			wantURL: "https://f.example/feed",
			wantInt: "30s",
		},
		{
			name:    "nil interval and sinks ignored",
			row:     map[string]any{"url": "https://g.example/feed", "interval": nil, "sinks": nil},
			wantURL: "https://g.example/feed",
		},
		{
			name:    "extra columns ignored",
			row:     map[string]any{"url": "https://h.example/feed", "enabled": true, "note": "hi"},
			wantURL: "https://h.example/feed",
		},
		{
			name:    "missing url errors",
			row:     map[string]any{"interval": "5m"},
			wantErr: true,
		},
		{
			name:    "blank url errors",
			row:     map[string]any{"url": "   "},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := specFromRow(tt.row)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got spec %+v", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.URL != tt.wantURL {
				t.Errorf("url: got %q want %q", got.URL, tt.wantURL)
			}
			if got.Interval != tt.wantInt {
				t.Errorf("interval: got %q want %q", got.Interval, tt.wantInt)
			}
			if len(got.Sinks) != len(tt.wantSink) {
				t.Fatalf("sinks: got %v want %v", got.Sinks, tt.wantSink)
			}
			for i := range got.Sinks {
				if got.Sinks[i] != tt.wantSink[i] {
					t.Errorf("sinks[%d]: got %q want %q", i, got.Sinks[i], tt.wantSink[i])
				}
			}
		})
	}
}

func TestNewPostgresValidation(t *testing.T) {
	ctx := context.Background()

	if _, err := NewPostgres(ctx, PostgresOptions{Name: "p"}); err == nil {
		t.Error("expected error when dsn is empty")
	}
	if _, err := NewPostgres(ctx, PostgresOptions{
		Name: "p", DSN: "postgres://u@h/db", Table: "feeds", Query: "SELECT 1",
	}); err == nil {
		t.Error("expected error when table and query both set")
	}
	if _, err := NewPostgres(ctx, PostgresOptions{
		Name: "p", DSN: "postgres://u@h/db", Table: "bad table!",
	}); err == nil {
		t.Error("expected error for invalid table identifier")
	}
}

func TestNewPostgresLazyConstruct(t *testing.T) {
	// pgxpool connects lazily, so construction with a syntactically valid DSN
	// succeeds without a reachable server. Verifies wiring/Name/Close.
	ctx := context.Background()
	p, err := NewPostgres(ctx, PostgresOptions{Name: "db-feeds", DSN: "postgres://u:p@localhost:5432/db"})
	if err != nil {
		t.Fatalf("construct: %v", err)
	}
	defer func() { _ = p.Close() }()
	if p.Name() != "db-feeds" {
		t.Errorf("name: got %q want %q", p.Name(), "db-feeds")
	}
	if p.Changes() == nil {
		t.Error("Changes() returned nil channel")
	}
}
