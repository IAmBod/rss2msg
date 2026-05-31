package feed

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/iambod/rss2msg/internal/model"
	"github.com/iambod/rss2msg/internal/sink"
)

// Compile-time assertion: Publisher satisfies sink.Publisher.
var _ sink.Publisher = (*Publisher)(nil)

// Options configures a feed Publisher. Storage fields mirror the validated
// config (internal/config FeedSinkConfig); the sink owns its own store.
type Options struct {
	Name            string
	Listen          string
	PublicURL       string
	Meta            FeedMeta
	MaxItems        int
	RSSPath         string
	AtomPath        string
	RenderCacheTTL  time.Duration
	CacheControlTTL time.Duration
	Timeouts        Timeouts
	TLSCertFile     string
	TLSKeyFile      string
	Auth            *AuthConfig

	StoreDriver string
	SQLitePath  string
	PostgresDSN string
	Table       string // table name for sqlite/postgres backends (default feed_output)
	PostgresTLS *PGTLSOptions
}

type Timeouts struct {
	ReadHeader, Read, Write, Idle, Shutdown time.Duration
}

func (t Timeouts) withDefaults() Timeouts {
	if t.ReadHeader == 0 {
		t.ReadHeader = 5 * time.Second
	}
	if t.Read == 0 {
		t.Read = 10 * time.Second
	}
	if t.Write == 0 {
		t.Write = 15 * time.Second
	}
	if t.Idle == 0 {
		t.Idle = 60 * time.Second
	}
	if t.Shutdown == 0 {
		t.Shutdown = 5 * time.Second
	}
	return t
}

type Publisher struct {
	name     string
	store    Store
	server   *http.Server
	ln       net.Listener
	shutdown time.Duration
	tlsCert  string
	tlsKey   string
}

func New(ctx context.Context, o Options) (*Publisher, error) {
	if o.Name == "" {
		return nil, fmt.Errorf("feed sink: name is required")
	}
	store, err := openStore(ctx, o)
	if err != nil {
		return nil, err
	}
	rss, atom := o.RSSPath, o.AtomPath
	if rss == "" {
		rss = "/rss"
	}
	if atom == "" {
		atom = "/atom"
	}
	selfBase := o.PublicURL
	if selfBase == "" {
		selfBase = o.Meta.Link
	}
	selfBase = strings.TrimRight(selfBase, "/")
	o.Meta.SelfRSS = selfBase + rss
	o.Meta.SelfAtom = selfBase + atom

	h := newHandler(handlerConfig{
		store: store, meta: o.Meta, maxItems: o.MaxItems,
		rssPath: rss, atomPath: atom,
		renderCacheTTL: o.RenderCacheTTL, cacheControlTTL: o.CacheControlTTL,
		auth: o.Auth, startedAt: time.Now(),
	})

	to := o.Timeouts.withDefaults()
	ln, err := net.Listen("tcp", o.Listen)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("feed sink %q: listen %q: %w", o.Name, o.Listen, err)
	}
	srv := &http.Server{
		Handler:           h,
		ReadHeaderTimeout: to.ReadHeader,
		ReadTimeout:       to.Read,
		WriteTimeout:      to.Write,
		IdleTimeout:       to.Idle,
	}
	p := &Publisher{name: o.Name, store: store, server: srv, ln: ln, shutdown: to.Shutdown, tlsCert: o.TLSCertFile, tlsKey: o.TLSKeyFile}
	go p.serve()
	return p, nil
}

func (p *Publisher) serve() {
	var err error
	if p.tlsCert != "" && p.tlsKey != "" {
		err = p.server.ServeTLS(p.ln, p.tlsCert, p.tlsKey)
	} else {
		err = p.server.Serve(p.ln)
	}
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		_ = err // server logging hook added in the telemetry task
	}
}

func (p *Publisher) Addr() string { return p.ln.Addr().String() }

func (p *Publisher) Name() string { return p.name }

func (p *Publisher) Publish(ctx context.Context, c model.Change) error {
	if c.DLQFromSink != "" {
		return nil // never surface error envelopes in a public feed
	}
	return p.store.Write(ctx, c)
}

func (p *Publisher) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), p.shutdown)
	defer cancel()
	err := p.server.Shutdown(ctx)
	if cerr := p.store.Close(); err == nil {
		err = cerr
	}
	return err
}

func openStore(ctx context.Context, o Options) (Store, error) {
	switch storeDriver(o.StoreDriver) {
	case "memory":
		return newMemoryStore(o.MaxItems), nil
	case "sqlite":
		return newSQLiteStore(ctx, o.SQLitePath, o.Table, o.MaxItems)
	case "postgres":
		return newPostgresStore(ctx, postgresOptions{DSN: o.PostgresDSN, Table: o.Table, Max: o.MaxItems, TLS: o.PostgresTLS})
	default:
		return nil, fmt.Errorf("feed sink %q: unknown store driver %q", o.Name, o.StoreDriver)
	}
}

func storeDriver(d string) string {
	if d == "" {
		return "memory"
	}
	return d
}
