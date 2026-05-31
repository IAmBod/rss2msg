package main

import (
	"context"
	"testing"

	"github.com/iambod/rss2msg/internal/config"
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
