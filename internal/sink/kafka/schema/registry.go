package schema

import (
	"context"
	"sync"

	"github.com/twmb/franz-go/pkg/sr"
)

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
		clientOpts = append(clientOpts, sr.DialTLSConfig(opts.TLS))
	}
	return sr.NewClient(clientOpts...)
}
