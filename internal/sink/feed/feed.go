package feed

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/quic-go/quic-go/http3"
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
	HTTP3           bool // serve HTTP/3 over QUIC alongside TCP; requires TLS
	RSSAuth         *SurfaceAuth
	AtomAuth        *SurfaceAuth
	MCPAuth         *SurfaceAuth

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
	h3       *http3.Server // nil unless HTTP/3 is enabled
	ln       net.Listener
	udpConn  *net.UDPConn // nil unless HTTP/3 is enabled
	shutdown time.Duration
	tlsCert  string
	tlsKey   string
	logger   zerolog.Logger
}

// altSvcHandler wraps the TCP (H1/H2) handler and advertises the HTTP/3
// endpoint via the Alt-Svc response header so clients can upgrade.
type altSvcHandler struct {
	next http.Handler
	h3   *http3.Server
}

func (a altSvcHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	_ = a.h3.SetQUICHeaders(w.Header())
	a.next.ServeHTTP(w, r)
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
		rssAuth: o.RSSAuth, atomAuth: o.AtomAuth, startedAt: time.Now(),
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
		mux.Handle(mcpPath, mcpAuthMiddleware(o.MCPAuth, h.instr, mcpCount, sh))
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

	if o.HTTP3 {
		// Serve the same root handler (which includes the MCP mux when enabled)
		// over HTTP/3, not just the rss/atom handler.
		if err := p.enableHTTP3(root); err != nil {
			_ = ln.Close()
			_ = store.Close()
			return nil, fmt.Errorf("feed sink %q: %w", o.Name, err)
		}
	}

	go p.serve()
	return p, nil
}

// enableHTTP3 binds a UDP socket on the same host:port as the TCP listener and
// starts an HTTP/3 server on it. The TCP handler is wrapped to advertise the
// HTTP/3 endpoint via Alt-Svc. Requires a TLS cert/key pair.
func (p *Publisher) enableHTTP3(h http.Handler) error {
	if p.tlsCert == "" || p.tlsKey == "" {
		return errors.New("http3 requires tls cert_file and key_file")
	}
	cert, err := tls.LoadX509KeyPair(p.tlsCert, p.tlsKey)
	if err != nil {
		return fmt.Errorf("http3: load keypair: %w", err)
	}
	tcpAddr, ok := p.ln.Addr().(*net.TCPAddr)
	if !ok {
		return fmt.Errorf("http3: listener addr is %T, want *net.TCPAddr", p.ln.Addr())
	}
	udpConn, err := net.ListenUDP("udp", &net.UDPAddr{IP: tcpAddr.IP, Port: tcpAddr.Port})
	if err != nil {
		return fmt.Errorf("http3: listen udp on %s: %w", tcpAddr, err)
	}
	h3 := &http3.Server{
		Addr:      p.ln.Addr().String(),
		Handler:   h,
		TLSConfig: http3.ConfigureTLSConfig(&tls.Config{Certificates: []tls.Certificate{cert}}),
	}
	p.h3 = h3
	p.udpConn = udpConn
	// Advertise HTTP/3 on the TCP (H1/H2) responses.
	p.server.Handler = altSvcHandler{next: h, h3: h3}
	return nil
}

func (p *Publisher) serve() {
	p.logger.Info().Str("sink", p.name).Str("addr", p.ln.Addr().String()).Bool("http3", p.h3 != nil).Msg("feed sink listening")
	if p.h3 != nil {
		go func() {
			if err := p.h3.Serve(p.udpConn); err != nil && !errors.Is(err, http.ErrServerClosed) {
				p.logger.Error().Err(err).Str("sink", p.name).Msg("feed sink http3 server stopped")
			}
		}()
	}
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
	if p.h3 != nil {
		if cerr := p.h3.Close(); err == nil {
			err = cerr
		}
	}
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
