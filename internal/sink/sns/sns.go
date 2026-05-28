package sns

import (
	"context"
	"errors"

	"github.com/iambod/rss2msg/internal/model"
)

var ErrNotImplemented = errors.New("sns sink: not implemented in this version")

type Publisher struct{ name string }

func New(name string) *Publisher                                       { return &Publisher{name: name} }
func (p *Publisher) Name() string                                      { return p.name }
func (p *Publisher) Publish(ctx context.Context, _ model.Change) error { return ErrNotImplemented }
func (p *Publisher) Close() error                                      { return nil }
