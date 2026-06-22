package config

import (
	"strings"
	"testing"
)

func baseAdmin() Config {
	c := goodCfg()
	c.Admin.Enabled = true
	c.Admin.Auth = AdminAuthConfig{Enabled: true, BearerTokens: []FeedBearerCred{{Name: "ops", Token: "t"}}}
	return c
}

func TestAdminValidate(t *testing.T) {
	// happy path
	if _, err := Validate(baseAdmin()); err != nil {
		t.Fatalf("valid admin config errored: %v", err)
	}
	// no listen
	c := baseAdmin()
	c.Admin.Listen = ""
	if _, err := Validate(c); err == nil {
		t.Fatal("empty listen should error")
	}
	// auth enabled, no creds
	c = baseAdmin()
	c.Admin.Auth = AdminAuthConfig{Enabled: true}
	if _, err := Validate(c); err == nil {
		t.Fatal("auth enabled w/o creds should error")
	}
	// auth disabled, no mTLS => ok but warns
	c = baseAdmin()
	c.Admin.Auth = AdminAuthConfig{Enabled: false}
	warns, err := Validate(c)
	if err != nil {
		t.Fatalf("open admin should validate: %v", err)
	}
	if !hasWarn(warns, "no authentication") {
		t.Fatalf("expected open-API warning, got %v", warns)
	}
	// auth disabled + mTLS => ok, no open-API warning
	c = baseAdmin()
	c.Admin.Auth = AdminAuthConfig{Enabled: false}
	c.Admin.TLS = AdminTLSConfig{Enabled: true, CertFile: "c", KeyFile: "k", ClientCAFile: "ca"}
	warns, err = Validate(c)
	if err != nil || hasWarn(warns, "no authentication") {
		t.Fatalf("mTLS-only should be warning-free: warns=%v err=%v", warns, err)
	}
	// tls enabled w/o cert/key
	c = baseAdmin()
	c.Admin.TLS = AdminTLSConfig{Enabled: true}
	if _, err := Validate(c); err == nil {
		t.Fatal("tls.enabled w/o cert/key should error")
	}
	// client CA without tls.enabled
	c = baseAdmin()
	c.Admin.TLS = AdminTLSConfig{Enabled: false, ClientCAFile: "ca"}
	if _, err := Validate(c); err == nil {
		t.Fatal("client_ca_file w/o tls.enabled should error")
	}
	// bad CORS origin
	c = baseAdmin()
	c.Admin.CORS = AdminCORSConfig{AllowedOrigins: []string{"not a url"}}
	if _, err := Validate(c); err == nil {
		t.Fatal("malformed CORS origin should error")
	}
	// wildcard CORS warns
	c = baseAdmin()
	c.Admin.CORS = AdminCORSConfig{AllowedOrigins: []string{"*"}}
	warns, _ = Validate(c)
	if !hasWarn(warns, "wildcard") {
		t.Fatalf("wildcard CORS should warn, got %v", warns)
	}
}

func hasWarn(warns []string, sub string) bool {
	for _, w := range warns {
		if strings.Contains(w, sub) {
			return true
		}
	}
	return false
}
