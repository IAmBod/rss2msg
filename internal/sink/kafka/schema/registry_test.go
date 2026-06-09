package schema

import (
	"context"
	"testing"

	"github.com/twmb/franz-go/pkg/sr"
	"github.com/twmb/franz-go/pkg/sr/srfake"
)

func newTestRegistrar(t *testing.T, auto bool) (*registrar, *srfake.Registry) {
	t.Helper()
	fake := srfake.New()
	t.Cleanup(fake.Close)
	cl, err := sr.NewClient(sr.URLs(fake.URL()))
	if err != nil {
		t.Fatal(err)
	}
	r := &registrar{
		cl:      cl,
		subject: "feed.changes-value",
		schema:  sr.Schema{Schema: `{"type":"object"}`, Type: sr.TypeJSON},
		auto:    auto,
	}
	return r, fake
}

func TestRegistrarAutoRegisterCachesID(t *testing.T) {
	r, _ := newTestRegistrar(t, true)
	ctx := context.Background()
	id1, err := r.schemaID(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if id1 == 0 {
		t.Fatal("expected non-zero schema id")
	}
	id2, err := r.schemaID(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if id1 != id2 {
		t.Fatalf("id not cached: %d != %d", id1, id2)
	}
}

func TestRegistrarNoAutoRegisterErrorsWhenMissing(t *testing.T) {
	r, _ := newTestRegistrar(t, false)
	if _, err := r.schemaID(context.Background()); err == nil {
		t.Fatal("expected error looking up unregistered subject")
	}
}

func TestRegistrarNoAutoRegisterFindsSeeded(t *testing.T) {
	r, fake := newTestRegistrar(t, false)
	fake.SeedSchema("feed.changes-value", 1, 42, r.schema)
	id, err := r.schemaID(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if id != 42 {
		t.Fatalf("looked-up id = %d, want 42", id)
	}
}
