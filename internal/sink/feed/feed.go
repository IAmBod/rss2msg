package feed

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/metric"

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
	RSS             Surface
	Atom            Surface
	MCP             Surface
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

	Meter  metric.Meter   // optional; nil => no metrics
	Logger zerolog.Logger // optional; zero value => no server logging
}

// Surface is one output endpoint of the feed sink (RSS, Atom, or MCP). A
// disabled surface registers no route. Path defaults to the canonical value
// (def passed at wiring) when enabled but left empty.
type Surface struct {
	Enabled bool
	Path    string
}

// surfacePath returns the route path for a surface: empty when disabled (the
// handler 404s empty paths), or the configured path / canonical default.
func surfacePath(s Surface, def string) string {
	if !s.Enabled {
		return ""
	}
	if s.Path == "" {
		return def
	}
	return s.Path
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
	logger   zerolog.Logger
}

func New(ctx context.Context, o Options) (*Publisher, error) {
	if o.Name == "" {
		return nil, fmt.Errorf("feed sink: name is required")
	}
	store, err := openStore(ctx, o)
	if err != nil {
		return nil, err
	}
	// A disabled surface yields an empty path, which the handler treats as
	// "not served" (404). An enabled surface with no explicit path falls back
	// to the canonical default.
	rss := surfacePath(o.RSS, "/rss")
	atom := surfacePath(o.Atom, "/atom")
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

	if o.Meter != nil {
		instr, err := newInstruments(o.Meter)
		if err != nil {
			_ = store.Close()
			return nil, fmt.Errorf("feed sink %q: instruments: %w", o.Name, err)
		}
		h.instr = instr
	}

	// When the MCP surface is enabled, front the listener with a mux so /mcp
	// routes to the streamable MCP handler (behind the same auth) while the
	// rss/atom handler keeps serving everything else.
	var root http.Handler = h
	if mcpPath := surfacePath(o.MCP, "/mcp"); mcpPath != "" {
		var mcpCount metric.Int64Counter
		if h.instr != nil {
			mcpCount = h.instr.mcpRequests
		}
		ms := buildMCPServer(store, o.MaxItems, o.Name)
		sh := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return ms }, nil)
		mux := http.NewServeMux()
		mux.Handle("/", h)
		mux.Handle(mcpPath, mcpAuthMiddleware(o.Auth, mcpCount, sh))
		root = mux
	}

	to := o.Timeouts.withDefaults()
	ln, err := net.Listen("tcp", o.Listen)
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("feed sink %q: listen %q: %w", o.Name, o.Listen, err)
	}
	srv := &http.Server{
		Handler:           root,
		ReadHeaderTimeout: to.ReadHeader,
		ReadTimeout:       to.Read,
		WriteTimeout:      to.Write,
		IdleTimeout:       to.Idle,
	}
	p := &Publisher{name: o.Name, store: store, server: srv, ln: ln, shutdown: to.Shutdown, tlsCert: o.TLSCertFile, tlsKey: o.TLSKeyFile, logger: o.Logger}
	go p.serve()
	return p, nil
}

func (p *Publisher) serve() {
	p.logger.Info().Str("sink", p.name).Str("addr", p.ln.Addr().String()).Msg("feed sink listening")
	var err error
	if p.tlsCert != "" && p.tlsKey != "" {
		err = p.server.ServeTLS(p.ln, p.tlsCert, p.tlsKey)
	} else {
		err = p.server.Serve(p.ln)
	}
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		p.logger.Error().Err(err).Str("sink", p.name).Msg("feed sink server stopped")
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
