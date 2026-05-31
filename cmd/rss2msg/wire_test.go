package main

import (
	"context"
	"strings"
	"testing"

	"github.com/iambod/rss2msg/internal/config"
	"github.com/iambod/rss2msg/internal/model"
	"github.com/iambod/rss2msg/internal/sink"
	compositesink "github.com/iambod/rss2msg/internal/sink/composite"
	sinkstdout "github.com/iambod/rss2msg/internal/sink/stdout"
	"github.com/iambod/rss2msg/internal/telemetry"
	"github.com/stretchr/testify/require"
)

func TestRedisCoordOptionsByMode(t *testing.T) {
	o := redisCoordOptions(config.CoordinationConfig{Redis: config.CoordinationRedisConfig{
		Mode: "sentinel",
		Sentinel: config.CoordinationRedisSentinelConfig{
			MasterName: "m", Addrs: []string{"a:26379"}, Password: "p", SentinelPassword: "sp", DB: 1,
		},
	}})
	require.Equal(t, "sentinel", o.Mode)
	require.Equal(t, "m", o.Sentinel.MasterName)
	require.Equal(t, []string{"a:26379"}, o.Sentinel.Addrs)
	require.Equal(t, "p", o.Sentinel.Password)
	require.Equal(t, "sp", o.Sentinel.SentinelPassword)
	require.Equal(t, 1, o.Sentinel.DB)

	o = redisCoordOptions(config.CoordinationConfig{Redis: config.CoordinationRedisConfig{
		Mode: "cluster", Cluster: config.CoordinationRedisClusterConfig{Addrs: []string{"n:6379"}, Username: "u"},
	}})
	require.Equal(t, "cluster", o.Mode)
	require.Equal(t, []string{"n:6379"}, o.Cluster.Addrs)
	require.Equal(t, "u", o.Cluster.Username)
}

func TestWrapSinkComposite_IsPassThrough(t *testing.T) {
	reg := sink.NewRegistry()
	comp, _ := compositesink.New(compositesink.Options{Name: "fanout"})
	_ = reg.Add(comp)
	cfg := config.Config{
		Retry: config.RetryConfig{MaxAttempts: 5},
		Sinks: []config.SinkConfig{{Name: "fanout", Driver: "composite"}},
	}
	w, err := wrapSink(reg, cfg, "fanout")
	if err != nil {
		t.Fatal(err)
	}
	// A composite with no branches always succeeds; a single Deliver attempt.
	r := w.Deliver(context.Background(), model.Change{ItemID: "x"})
	if r.State != sink.BranchSuccess || r.Attempts != 1 {
		t.Fatalf("composite must be wrapped pass-through: state=%v attempts=%d", r.State, r.Attempts)
	}
}

func TestLinkComposites_FansOut(t *testing.T) {
	reg := sink.NewRegistry()
	a, _ := sinkstdout.New(sinkstdout.Options{Name: "a", Target: "stderr"})
	b, _ := sinkstdout.New(sinkstdout.Options{Name: "b", Target: "stderr"})
	comp, _ := compositesink.New(compositesink.Options{Name: "default"})
	_ = reg.Add(a)
	_ = reg.Add(b)
	_ = reg.Add(comp)
	cfg := config.Config{
		Retry: config.RetryConfig{MaxAttempts: 1},
		Sinks: []config.SinkConfig{
			{Name: "a", Driver: "stdout", Stdout: config.StdoutSinkConfig{Target: "stderr"}},
			{Name: "b", Driver: "stdout", Stdout: config.StdoutSinkConfig{Target: "stderr"}},
			{Name: "default", Driver: "composite", Composite: config.CompositeSinkConfig{Children: []string{"a", "b"}}},
		},
	}
	if err := linkComposites(reg, cfg); err != nil {
		t.Fatal(err)
	}
	if err := comp.Publish(context.Background(), model.Change{ItemID: "x"}); err != nil {
		t.Fatalf("composite publish: %v", err)
	}
}

func TestLinkComposites_Nested(t *testing.T) {
	// outer -> inner -> leaf. Linking must resolve the nested composite child
	// (inner) via wrapSink's pass-through branch, and publishing on outer must
	// reach the leaf without error.
	reg := sink.NewRegistry()
	leaf, _ := sinkstdout.New(sinkstdout.Options{Name: "leaf", Target: "stderr"})
	inner, _ := compositesink.New(compositesink.Options{Name: "inner"})
	outer, _ := compositesink.New(compositesink.Options{Name: "outer"})
	_ = reg.Add(leaf)
	_ = reg.Add(inner)
	_ = reg.Add(outer)
	cfg := config.Config{
		Retry: config.RetryConfig{MaxAttempts: 1},
		Sinks: []config.SinkConfig{
			{Name: "leaf", Driver: "stdout", Stdout: config.StdoutSinkConfig{Target: "stderr"}},
			{Name: "inner", Driver: "composite", Composite: config.CompositeSinkConfig{Children: []string{"leaf"}}},
			{Name: "outer", Driver: "composite", Composite: config.CompositeSinkConfig{Children: []string{"inner"}}},
		},
	}
	if err := linkComposites(reg, cfg); err != nil {
		t.Fatal(err)
	}
	if err := outer.Publish(context.Background(), model.Change{ItemID: "x"}); err != nil {
		t.Fatalf("nested composite publish: %v", err)
	}
}

func TestWrapSink_UnknownName(t *testing.T) {
	reg := sink.NewRegistry()
	_, err := wrapSink(reg, config.Config{}, "nope")
	if err == nil || !strings.Contains(err.Error(), "nope") {
		t.Fatalf("expected unknown-sink error naming the target, got %v", err)
	}
}

func TestLinkComposites_UnknownChild(t *testing.T) {
	reg := sink.NewRegistry()
	comp, _ := compositesink.New(compositesink.Options{Name: "default"})
	_ = reg.Add(comp)
	cfg := config.Config{Sinks: []config.SinkConfig{
		{Name: "default", Driver: "composite", Composite: config.CompositeSinkConfig{Children: []string{"missing"}}},
	}}
	err := linkComposites(reg, cfg)
	if err == nil || !strings.Contains(err.Error(), "missing") {
		t.Fatalf("expected child-resolution error naming the missing child, got %v", err)
	}
}

func TestBuildPublisher_Feed(t *testing.T) {
	tel := &telemetry.Telemetry{} // zero Meter/Logger; feed New tolerates them
	sc := config.SinkConfig{Name: "out", Driver: "feed", Feed: config.FeedSinkConfig{
		Listen: "127.0.0.1:0", Link: "https://x/", MaxItems: 5,
		Store: config.FeedStoreConfig{Driver: "memory"},
	}}
	p, err := buildPublisher(context.Background(), sc, tel)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if p.Name() != "out" {
		t.Fatalf("name: %s", p.Name())
	}
}
