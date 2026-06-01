//go:build integration

// Package gcplocal boots a Google Cloud Pub/Sub emulator testcontainer for
// gcp_pubsub sink integration tests.
package gcplocal

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// ProjectID is the project the emulator is started with; topics/subscriptions
// are created under it.
const ProjectID = "rss2msg-test"

// PubSub holds the running emulator container and its gRPC endpoint
// (host:port) suitable for option.WithEndpoint.
type PubSub struct {
	Container testcontainers.Container
	Endpoint  string
}

// Run starts a Pub/Sub emulator container and returns its endpoint.
func Run(ctx context.Context, t *testing.T) *PubSub {
	t.Helper()
	req := testcontainers.ContainerRequest{
		Image:        "gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators",
		ExposedPorts: []string{"8085/tcp"},
		Cmd: []string{
			"gcloud", "beta", "emulators", "pubsub", "start",
			"--host-port=0.0.0.0:8085",
			"--project=" + ProjectID,
		},
		WaitingFor: wait.ForAll(
			wait.ForListeningPort("8085/tcp"),
			wait.ForLog("Server started, listening on").WithStartupTimeout(120*time.Second),
		),
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("pubsub emulator run: %v", err)
	}
	t.Cleanup(func() { _ = c.Terminate(ctx) })

	host, err := c.Host(ctx)
	if err != nil {
		t.Fatalf("pubsub emulator host: %v", err)
	}
	mp, err := c.MappedPort(ctx, "8085/tcp")
	if err != nil {
		t.Fatalf("pubsub emulator port: %v", err)
	}
	return &PubSub{
		Container: c,
		Endpoint:  fmt.Sprintf("%s:%s", host, mp.Port()),
	}
}
