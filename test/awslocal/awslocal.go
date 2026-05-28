//go:build integration

// Package awslocal boots a LocalStack testcontainer for SQS+SNS integration
// tests across multiple packages.
package awslocal

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// LocalStack holds the running container and an exposed endpoint URL.
type LocalStack struct {
	Container testcontainers.Container
	Endpoint  string
}

// Run starts a LocalStack container with SQS and SNS enabled. The endpoint
// URL is suitable for both services via the SDK's BaseEndpoint override.
func Run(ctx context.Context, t *testing.T) *LocalStack {
	t.Helper()
	req := testcontainers.ContainerRequest{
		Image:        "localstack/localstack:3.6",
		ExposedPorts: []string{"4566/tcp"},
		Env: map[string]string{
			"SERVICES":             "sqs,sns",
			"DEBUG":                "0",
			"AWS_DEFAULT_REGION":   "us-east-1",
			"DISABLE_CORS_CHECKS":  "1",
			"SKIP_INFRA_DOWNLOADS": "1",
		},
		WaitingFor: wait.ForAll(
			wait.ForListeningPort("4566/tcp"),
			wait.ForLog("Ready.").WithStartupTimeout(120*time.Second),
		),
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("localstack run: %v", err)
	}
	t.Cleanup(func() { _ = c.Terminate(ctx) })

	host, err := c.Host(ctx)
	if err != nil {
		t.Fatalf("localstack host: %v", err)
	}
	mp, err := c.MappedPort(ctx, "4566/tcp")
	if err != nil {
		t.Fatalf("localstack port: %v", err)
	}
	return &LocalStack{
		Container: c,
		Endpoint:  fmt.Sprintf("http://%s:%s", host, mp.Port()),
	}
}
