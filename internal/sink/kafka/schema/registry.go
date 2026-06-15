package schema

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"sync"

	"github.com/rs/zerolog/log"
	"github.com/twmb/franz-go/pkg/sr"
)

// TLSOptions configures TLS to the Schema Registry. Same shape as the kafka
// sink's TLS options for a consistent operator surface.
type TLSOptions struct {
	CAFile             string
	CertFile           string
	KeyFile            string
	ServerName         string
	InsecureSkipVerify bool
}

// buildTLSConfig translates TLSOptions into a *tls.Config.
func buildTLSConfig(opts TLSOptions) (*tls.Config, error) {
	tc := &tls.Config{
		InsecureSkipVerify: opts.InsecureSkipVerify, //nolint:gosec // opt-in, logged in newClient
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

// registrar resolves and caches the Schema Registry id for one subject.
// Registration is lazy (first Encode) so a registry blip at startup does not
// block process boot; on failure the id stays uncached and the next publish
// retries.
type registrar struct {
	cl      *sr.Client
	subject string
	schema  sr.Schema
	auto    bool

	mu sync.Mutex
	id int // 0 = not yet resolved
}

func (r *registrar) schemaID(ctx context.Context) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.id != 0 {
		return r.id, nil
	}
	var (
		ss  sr.SubjectSchema
		err error
	)
	if r.auto {
		ss, err = r.cl.CreateSchema(ctx, r.subject, r.schema)
	} else {
		ss, err = r.cl.LookupSchema(ctx, r.subject, r.schema)
	}
	if err != nil {
		return 0, err
	}
	r.id = ss.ID
	return r.id, nil
}

// newClient builds an sr.Client from Options.
func newClient(opts Options) (*sr.Client, error) {
	clientOpts := []sr.ClientOpt{sr.URLs(opts.URL)}
	if opts.BasicUser != "" || opts.BasicPass != "" {
		clientOpts = append(clientOpts, sr.BasicAuth(opts.BasicUser, opts.BasicPass))
	}
	if opts.TLS != nil {
		tc, err := buildTLSConfig(*opts.TLS)
		if err != nil {
			return nil, fmt.Errorf("schema registry tls: %w", err)
		}
		clientOpts = append(clientOpts, sr.DialTLSConfig(tc))
		if opts.TLS.InsecureSkipVerify {
			log.Warn().
				Str("sink_driver", "kafka").
				Str("component", "schema_registry").
				Msg("schema registry: TLS verification disabled (insecure_skip_verify=true)")
		}
	}
	return sr.NewClient(clientOpts...)
}
