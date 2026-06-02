package nats

import "testing"

func TestNewRejectsMissingName(t *testing.T) {
	t.Parallel()
	if _, err := New(Options{URL: "nats://localhost:4222", Subject: "feed.changes"}); err == nil {
		t.Fatal("expected error for missing name")
	}
}

func TestNewRejectsMissingURL(t *testing.T) {
	t.Parallel()
	if _, err := New(Options{Name: "x", Subject: "feed.changes"}); err == nil {
		t.Fatal("expected error for missing url")
	}
}

func TestNewRejectsMissingSubject(t *testing.T) {
	t.Parallel()
	if _, err := New(Options{Name: "x", URL: "nats://localhost:4222"}); err == nil {
		t.Fatal("expected error for missing subject")
	}
}

func TestNewRejectsConflictingAuth(t *testing.T) {
	t.Parallel()
	if _, err := New(Options{
		Name: "x", URL: "nats://localhost:4222", Subject: "s",
		Token: "tok", Username: "u", Password: "p",
	}); err == nil {
		t.Fatal("expected error for token + user/password")
	}
}

func TestNewRejectsPartialUserPassword(t *testing.T) {
	t.Parallel()
	if _, err := New(Options{
		Name: "x", URL: "nats://localhost:4222", Subject: "s", Username: "u",
	}); err == nil {
		t.Fatal("expected error for username without password")
	}
}

func TestNewRejectsCredsFileWithToken(t *testing.T) {
	t.Parallel()
	if _, err := New(Options{
		Name: "x", URL: "nats://localhost:4222", Subject: "s",
		Token: "tok", CredsFile: "/tmp/x.creds",
	}); err == nil {
		t.Fatal("expected error for token + creds_file")
	}
}
