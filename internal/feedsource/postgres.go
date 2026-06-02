package feedsource

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"

	"github.com/iambod/rss2msg/internal/config"
)

// Compile-time assertion that *Postgres satisfies Source.
var _ Source = (*Postgres)(nil)

const defaultPostgresTable = "feeds"

// PostgresOptions configures a Postgres-backed feed source. The source reads the
// desired feed list from an operator-managed table (it never creates or migrates
// the table) and polls it on Interval.
type PostgresOptions struct {
	Name     string
	DSN      string // required; pgx-style URL or keyword DSN
	Table    string // default "feeds"; validated identifier; mutually exclusive with Query
	Query    string // raw SQL override; mutually exclusive with Table
	Interval time.Duration
	TLS      *PostgresTLSOptions
}

// PostgresTLSOptions forces TLS for the source pool, mirroring the state/coord
// Postgres TLS surface. Setting any field drops pgx's plaintext fallbacks.
type PostgresTLSOptions struct {
	CAFile, CertFile, KeyFile, ServerName string
	InsecureSkipVerify                    bool
}

// Postgres is a feed source backed by a Postgres table. It composes Poll for the
// interval ticker and owns the connection pool. Each poll re-runs the query and
// maps every row to a FeedSpec (url required; interval and sinks optional).
type Postgres struct {
	pool  *pgxpool.Pool
	query string
	poll  *Poll
}

// NewPostgres opens a lazily-connecting pool against opts.DSN and returns a
// polling source. Construction validates options and the DSN syntax but does not
// require the server to be reachable.
func NewPostgres(ctx context.Context, opts PostgresOptions) (*Postgres, error) {
	if strings.TrimSpace(opts.DSN) == "" {
		return nil, fmt.Errorf("postgres feed source %q: dsn is required", opts.Name)
	}
	if opts.Table != "" && opts.Query != "" {
		return nil, fmt.Errorf("postgres feed source %q: table and query are mutually exclusive", opts.Name)
	}
	query := opts.Query
	if query == "" {
		table := opts.Table
		if table == "" {
			table = defaultPostgresTable
		}
		if !validPGIdentifier(table) {
			return nil, fmt.Errorf("postgres feed source %q: invalid table %q", opts.Name, table)
		}
		query = "SELECT * FROM " + table
	}

	cfg, err := pgxpool.ParseConfig(opts.DSN)
	if err != nil {
		return nil, fmt.Errorf("postgres feed source %q: parse dsn: %w", opts.Name, err)
	}
	if opts.TLS != nil {
		tc, err := buildPostgresTLS(*opts.TLS, cfg.ConnConfig.Host)
		if err != nil {
			return nil, fmt.Errorf("postgres feed source %q: %w", opts.Name, err)
		}
		cfg.ConnConfig.TLSConfig = tc
		// Drop plaintext fallbacks pgx set up from the DSN's sslmode — the
		// operator opted into TLS, so plaintext must never be attempted
		// (mirrors state/postgres and coord/postgres).
		cfg.ConnConfig.Fallbacks = nil
		if opts.TLS.InsecureSkipVerify {
			log.Warn().
				Str("component", "feedsource/postgres").
				Str("source", opts.Name).
				Msg("postgres feed source: TLS verification disabled (insecure_skip_verify=true)")
		}
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("postgres feed source %q: pgxpool: %w", opts.Name, err)
	}

	p := &Postgres{pool: pool, query: query}
	p.poll = NewPoll(opts.Name, opts.Interval, p.fetch)
	return p, nil
}

func (p *Postgres) Name() string { return p.poll.Name() }

func (p *Postgres) Feeds(ctx context.Context) ([]config.FeedConfig, error) { return p.fetch(ctx) }

func (p *Postgres) Changes() <-chan struct{} { return p.poll.Changes() }

// Close stops the poll ticker and closes the connection pool.
func (p *Postgres) Close() error {
	p.poll.Close()
	p.pool.Close()
	return nil
}

func (p *Postgres) fetch(ctx context.Context) ([]config.FeedConfig, error) {
	rows, err := p.pool.Query(ctx, p.query)
	if err != nil {
		return nil, fmt.Errorf("postgres feed source %q: query: %w", p.poll.Name(), err)
	}
	maps, err := pgx.CollectRows(rows, pgx.RowToMap)
	if err != nil {
		return nil, fmt.Errorf("postgres feed source %q: scan: %w", p.poll.Name(), err)
	}
	specs := make([]FeedSpec, 0, len(maps))
	for i, m := range maps {
		spec, err := specFromRow(m)
		if err != nil {
			return nil, fmt.Errorf("postgres feed source %q: row %d: %w", p.poll.Name(), i, err)
		}
		specs = append(specs, spec)
	}
	return SpecsToConfigs(specs)
}

// specFromRow builds a FeedSpec from a scanned row map. Only the url, interval,
// and sinks keys are consulted; any other columns are ignored. url is required.
func specFromRow(m map[string]any) (FeedSpec, error) {
	url, err := rowString(m, "url")
	if err != nil {
		return FeedSpec{}, err
	}
	if strings.TrimSpace(url) == "" {
		return FeedSpec{}, fmt.Errorf("url is required")
	}
	interval, err := rowString(m, "interval")
	if err != nil {
		return FeedSpec{}, err
	}
	sinks, err := rowStringSlice(m, "sinks")
	if err != nil {
		return FeedSpec{}, err
	}
	return FeedSpec{URL: url, Interval: strings.TrimSpace(interval), Sinks: sinks}, nil
}

// rowString reads a text-ish column as a string. Missing/NULL reads as "".
func rowString(m map[string]any, key string) (string, error) {
	v, ok := m[key]
	if !ok || v == nil {
		return "", nil
	}
	switch s := v.(type) {
	case string:
		return s, nil
	case []byte:
		return string(s), nil
	default:
		return "", fmt.Errorf("column %q: expected text, got %T", key, v)
	}
}

// rowStringSlice reads a sinks column. Accepts a Postgres text[] (decoded by
// pgx.RowToMap as []string or []interface{}), a JSON array string, or a single
// bare sink name. Missing/NULL reads as nil.
func rowStringSlice(m map[string]any, key string) ([]string, error) {
	v, ok := m[key]
	if !ok || v == nil {
		return nil, nil
	}
	switch s := v.(type) {
	case []string:
		return s, nil
	case []interface{}:
		out := make([]string, 0, len(s))
		for _, e := range s {
			switch ev := e.(type) {
			case string:
				out = append(out, ev)
			case []byte:
				out = append(out, string(ev))
			default:
				return nil, fmt.Errorf("column %q: element %T is not text", key, e)
			}
		}
		return out, nil
	case string:
		return parseSinkText(s), nil
	case []byte:
		return parseSinkText(string(s)), nil
	default:
		return nil, fmt.Errorf("column %q: expected text[] or text, got %T", key, v)
	}
}

// parseSinkText interprets a scalar sinks value: a JSON array yields its
// elements; anything else is treated as a single sink name (empty -> nil).
func parseSinkText(s string) []string {
	trimmed := strings.TrimSpace(s)
	if trimmed == "" {
		return nil
	}
	if strings.HasPrefix(trimmed, "[") {
		var arr []string
		if err := json.Unmarshal([]byte(trimmed), &arr); err == nil {
			return arr
		}
	}
	return []string{trimmed}
}

// validPGIdentifier reports whether s is a safe unquoted SQL identifier. Kept
// local to feedsource; mirrors the helper in internal/sink/feed.
func validPGIdentifier(s string) bool {
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

// buildPostgresTLS translates PostgresTLSOptions into a *tls.Config.
// defaultServerName is the host parsed from the DSN, used as SNI unless overridden.
func buildPostgresTLS(opts PostgresTLSOptions, defaultServerName string) (*tls.Config, error) {
	tc := &tls.Config{
		ServerName:         defaultServerName,
		InsecureSkipVerify: opts.InsecureSkipVerify, //nolint:gosec // operator opt-in, logged at warn
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
