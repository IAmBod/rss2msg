package postgres

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"

	"github.com/iambod/rss2msg/internal/model"
)

type Publisher struct {
	name  string
	pool  *pgxpool.Pool
	table string
}

type Options struct {
	Name  string
	DSN   string
	Table string

	// TLS, if non-nil, applies custom TLS to the pool and forces TLS by
	// clearing pgx's plaintext fallbacks (so plaintext is never attempted),
	// overriding whatever the DSN's sslmode produced.
	TLS *TLSOptions
}

// TLSOptions configures custom TLS for the sink's Postgres pool. Same shape as
// the coordinator / state-store options so operators have a consistent surface.
type TLSOptions struct {
	// CAFile is a PEM-encoded CA bundle to trust instead of the system roots.
	CAFile string
	// CertFile and KeyFile together enable client-certificate (mTLS) auth.
	// Either both or neither must be set.
	CertFile string
	KeyFile  string
	// ServerName overrides the SNI / certificate verification hostname.
	// Defaults to the DSN host.
	ServerName string
	// InsecureSkipVerify disables server certificate verification. For
	// local/test only — logged at warn.
	InsecureSkipVerify bool
}

const defaultTable = "feed_changes"

func New(ctx context.Context, opts Options) (*Publisher, error) {
	if opts.Name == "" {
		return nil, fmt.Errorf("postgres sink: name is required")
	}
	if opts.DSN == "" {
		return nil, fmt.Errorf("postgres sink %q: dsn is required", opts.Name)
	}
	table := opts.Table
	if table == "" {
		table = defaultTable
	}
	if !validIdentifier(table) {
		return nil, fmt.Errorf("postgres sink %q: invalid table name %q", opts.Name, table)
	}
	cfg, err := pgxpool.ParseConfig(opts.DSN)
	if err != nil {
		return nil, fmt.Errorf("postgres sink %q: parse dsn: %w", opts.Name, err)
	}
	if opts.TLS != nil {
		tc, err := buildTLSConfig(*opts.TLS, cfg.ConnConfig.Host)
		if err != nil {
			return nil, fmt.Errorf("postgres sink %q: build TLS config: %w", opts.Name, err)
		}
		cfg.ConnConfig.TLSConfig = tc
		// Drop any plaintext fallbacks pgx set up from the DSN's sslmode — the
		// operator opted into TLS knobs, so plaintext must never be attempted.
		cfg.ConnConfig.Fallbacks = nil
		if opts.TLS.InsecureSkipVerify {
			log.Warn().
				Str("sink", opts.Name).
				Str("sink_driver", "postgres").
				Msg("postgres sink: TLS verification disabled (insecure_skip_verify=true)")
		}
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("postgres sink %q: pgxpool: %w", opts.Name, err)
	}
	p := &Publisher{name: opts.Name, pool: pool, table: table}
	if err := p.migrate(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return p, nil
}

func (p *Publisher) Name() string { return p.name }

func (p *Publisher) Close() error { p.pool.Close(); return nil }

func (p *Publisher) Publish(ctx context.Context, change model.Change) error {
	payload, err := json.Marshal(change)
	if err != nil {
		return fmt.Errorf("postgres sink %q: marshal: %w", p.name, err)
	}
	// Table comes from validated config; validIdentifier guards the rest.
	stmt := fmt.Sprintf(`
        INSERT INTO %s (feed_url, item_id, kind, payload, detected_at)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (feed_url, item_id, detected_at) DO NOTHING
    `, p.table)
	_, err = p.pool.Exec(ctx, stmt, change.FeedURL, change.ItemID, string(change.Kind), payload, change.DetectedAt)
	return err
}

func (p *Publisher) migrate(ctx context.Context) error {
	stmt := fmt.Sprintf(`
        CREATE TABLE IF NOT EXISTS %s (
            feed_url TEXT NOT NULL,
            item_id TEXT NOT NULL,
            kind TEXT NOT NULL,
            payload JSONB NOT NULL,
            detected_at TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (feed_url, item_id, detected_at)
        );`, p.table)
	_, err := p.pool.Exec(ctx, stmt)
	return err
}

// buildTLSConfig translates TLSOptions into a *tls.Config. defaultServerName is
// the host parsed out of the DSN; used as SNI when the caller did not override
// it.
func buildTLSConfig(opts TLSOptions, defaultServerName string) (*tls.Config, error) {
	tc := &tls.Config{
		ServerName:         defaultServerName,
		InsecureSkipVerify: opts.InsecureSkipVerify, //nolint:gosec // opt-in, logged at warn
	}
	if opts.ServerName != "" {
		tc.ServerName = opts.ServerName
	}
	if opts.CAFile != "" {
		pem, err := os.ReadFile(opts.CAFile)
		if err != nil {
			return nil, fmt.Errorf("ca_file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("ca_file %q: no PEM certificates parsed", opts.CAFile)
		}
		tc.RootCAs = pool
	}
	if opts.CertFile != "" || opts.KeyFile != "" {
		if opts.CertFile == "" || opts.KeyFile == "" {
			return nil, fmt.Errorf("cert_file and key_file must both be set or both empty")
		}
		cert, err := tls.LoadX509KeyPair(opts.CertFile, opts.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("client cert: %w", err)
		}
		tc.Certificates = []tls.Certificate{cert}
	}
	return tc, nil
}

func validIdentifier(s string) bool {
	if s == "" || len(s) > 63 {
		return false
	}
	for i, r := range s {
		switch {
		case r == '_':
		case r >= 'a' && r <= 'z':
		case r >= 'A' && r <= 'Z':
		case r >= '0' && r <= '9' && i > 0:
		default:
			return false
		}
	}
	return true
}
