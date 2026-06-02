// Package grpc implements a sink that delivers each Change to a gRPC server
// implementing the rss2msg ChangeSink contract (proto/sink/v1). rss2msg is the
// client: it dials a server you run and calls Publish once per change. This is
// the typed analogue of the HTTP/webhook sink.
package grpc

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/timestamppb"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"

	"github.com/iambod/rss2msg/internal/model"
	sinkv1 "github.com/iambod/rss2msg/proto/sink/v1"
)

// TLSOptions configures transport security for the dialled connection. The
// shape matches the canonical sink TLS block.
type TLSOptions struct {
	CAFile             string
	CertFile           string
	KeyFile            string
	ServerName         string
	InsecureSkipVerify bool
}

// Options configures the gRPC sink.
type Options struct {
	Name string
	// Target is the gRPC dial target, typically host:port.
	Target string
	// Authority overrides the :authority pseudo-header (and TLS server name
	// when no explicit TLS.ServerName is set). Optional.
	Authority string
	// Timeout bounds each Publish RPC. Zero means no per-call deadline (the
	// caller's context still applies).
	Timeout time.Duration
	// Metadata is static outgoing metadata attached to every call (e.g.
	// authorization). Canonical per-change metadata overrides colliding keys.
	Metadata map[string]string
	// TLS, when non-nil, dials with transport security; otherwise the
	// connection is insecure (plaintext h2c).
	TLS *TLSOptions
}

// Publisher delivers Changes over the ChangeSink gRPC service.
type Publisher struct {
	name     string
	conn     *grpc.ClientConn
	client   sinkv1.ChangeSinkClient
	timeout  time.Duration
	metadata map[string]string
}

// New validates opts, dials the target, and returns a ready Publisher.
func New(opts Options) (*Publisher, error) {
	if strings.TrimSpace(opts.Name) == "" {
		return nil, fmt.Errorf("grpc sink: name is required")
	}
	if strings.TrimSpace(opts.Target) == "" {
		return nil, fmt.Errorf("grpc sink %q: target is required", opts.Name)
	}

	dialOpts := []grpc.DialOption{
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	}
	if opts.Authority != "" {
		dialOpts = append(dialOpts, grpc.WithAuthority(opts.Authority))
	}
	if opts.TLS != nil {
		tc, err := buildTLSConfig(*opts.TLS)
		if err != nil {
			return nil, fmt.Errorf("grpc sink %q: tls: %w", opts.Name, err)
		}
		if tc.ServerName == "" && opts.Authority != "" {
			tc.ServerName = opts.Authority
		}
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(credentials.NewTLS(tc)))
	} else {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	conn, err := grpc.NewClient(opts.Target, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("grpc sink %q: dial %q: %w", opts.Name, opts.Target, err)
	}

	return &Publisher{
		name:     opts.Name,
		conn:     conn,
		client:   sinkv1.NewChangeSinkClient(conn),
		timeout:  opts.Timeout,
		metadata: opts.Metadata,
	}, nil
}

func (p *Publisher) Name() string { return p.name }

func (p *Publisher) Close() error { return p.conn.Close() }

// Publish maps the Change to its protobuf form and calls ChangeSink.Publish. A
// non-OK gRPC status or an ack with accepted=false is returned as an error so
// the retry / dead-letter wrapper can handle it.
func (p *Publisher) Publish(ctx context.Context, change model.Change) error {
	if p.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, p.timeout)
		defer cancel()
	}

	// Static metadata first so the canonical per-change pairs below override
	// any colliding keys — operators can supply authorization etc., but cannot
	// clobber the fields that describe the change.
	kv := make([]string, 0, len(p.metadata)*2+12)
	for k, v := range p.metadata {
		kv = append(kv, k, v)
	}
	kv = append(kv,
		"rss2msg-schema-version", strconv.Itoa(change.SchemaVersion),
		"rss2msg-feed-url", change.FeedURL,
		"rss2msg-item-id", change.ItemID,
		"rss2msg-kind", string(change.Kind),
	)
	if change.DLQFromSink != "" {
		kv = append(kv,
			"rss2msg-dlq-from-sink", change.DLQFromSink,
			"rss2msg-dlq-error", change.DLQError,
			"rss2msg-dlq-attempts", strconv.Itoa(change.DLQAttempts),
		)
	}
	ctx = metadata.AppendToOutgoingContext(ctx, kv...)

	ack, err := p.client.Publish(ctx, &sinkv1.PublishRequest{Change: toProto(change)})
	if err != nil {
		return fmt.Errorf("grpc sink %q: publish: %w", p.name, err)
	}
	if ack != nil && !ack.GetAccepted() {
		if msg := ack.GetError(); msg != "" {
			return fmt.Errorf("grpc sink %q: server rejected change: %s", p.name, msg)
		}
		return fmt.Errorf("grpc sink %q: server rejected change", p.name)
	}
	return nil
}

// toProto mirrors model.Change into the wire message. Optional *time.Time
// fields map to absent Timestamps; the schema version is normalised the same
// way model.Change.MarshalJSON does.
func toProto(c model.Change) *sinkv1.Change {
	sv := c.SchemaVersion
	if sv == 0 {
		sv = model.SchemaVersion
	}
	pc := &sinkv1.Change{
		SchemaVersion: int32(sv),
		FeedUrl:       c.FeedURL,
		FeedTitle:     c.FeedTitle,
		ItemId:        c.ItemID,
		Kind:          string(c.Kind),
		Title:         c.Title,
		Link:          c.Link,
		Authors:       c.Authors,
		Summary:       c.Summary,
		Content:       c.Content,
		Categories:    c.Categories,
		ContentHash:   c.ContentHash,
		DetectedAt:    timestamppb.New(c.DetectedAt),
		DlqFromSink:   c.DLQFromSink,
		DlqError:      c.DLQError,
		DlqAttempts:   int32(c.DLQAttempts),
	}
	if c.PublishedAt != nil {
		pc.PublishedAt = timestamppb.New(*c.PublishedAt)
	}
	if c.UpdatedAt != nil {
		pc.UpdatedAt = timestamppb.New(*c.UpdatedAt)
	}
	return pc
}

// buildTLSConfig translates TLSOptions into a *tls.Config.
func buildTLSConfig(opts TLSOptions) (*tls.Config, error) {
	tc := &tls.Config{
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
