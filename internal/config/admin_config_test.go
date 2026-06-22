package config

import "testing"

func TestAdminDefaults(t *testing.T) {
	d := Defaults()
	if d.Admin.Enabled {
		t.Fatal("admin.enabled should default false")
	}
	if d.Admin.Listen != ":8090" {
		t.Fatalf("admin.listen default = %q", d.Admin.Listen)
	}
	if !d.Admin.Auth.Enabled {
		t.Fatal("admin.auth.enabled should default true")
	}
}

func TestAdminAuthToHTTPAuth(t *testing.T) {
	a := AdminAuthConfig{
		Enabled:      true,
		BearerTokens: []FeedBearerCred{{Name: "ops", Token: "tok"}},
		BasicUsers:   []FeedBasicAuthConfig{{Name: "u", Username: "a", Password: "p"}},
		APIKeys:      []FeedAPIKeyCred{{Name: "k", Key: "key"}},
		APIKeyHeader: "X-Token",
	}
	got := a.ToHTTPAuth()
	if got == nil || got.Empty() {
		t.Fatal("expected populated Auth")
	}
	if got.APIKeyHeader != "X-Token" || got.BearerTokens[0].Secret != "tok" || got.BasicUsers[0].Username != "a" || got.APIKeys[0].Secret != "key" {
		t.Fatalf("conversion mismatch: %+v", got)
	}
	// Disabled => empty (pass-through) auth.
	if !(AdminAuthConfig{Enabled: false, BearerTokens: []FeedBearerCred{{Token: "x"}}}).ToHTTPAuth().Empty() {
		t.Fatal("disabled auth should be Empty()")
	}
}
