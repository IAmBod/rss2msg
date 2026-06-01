---
title: Deploy on AWS ECS (Fargate)
type: how-to
tags: [rss2msg/docs, operations, deployment, aws]
summary: Run rss2msg as an ECS/Fargate service — task definition, config packaging, secrets from Secrets Manager, IAM task role for AWS sinks, and a scheduled run-once task.
updated: 2026-06-01
---

# Deploy on AWS ECS (Fargate)

Run the published rss2msg image as a long-lived ECS service on Fargate. For the
config, secrets, and observability model behind these knobs, see
[Deploy in Production](../deploy.md).

## Packaging config

ECS has no host bind mount, so supply `config.yaml` one of two ways:

1. **Bake a config image** — layer your config onto the published base (keep
   secrets out; they arrive as env vars via `${VAR}`):

   ```dockerfile
   FROM ghcr.io/iambod/rss2msg:latest
   COPY config.yaml /etc/rss2msg/config.yaml
   ```

   Push it to ECR and reference that image in the task definition. The base image
   already resolves `/etc/rss2msg/config.yaml` by default (see
   [Run with Docker](../run-with-docker.md)).
2. **Mount from EFS** — attach an EFS volume and mount it at `/etc/rss2msg`.

## Task definition

The container's default command is `serve`. Pull secrets from AWS Secrets Manager
(or SSM Parameter Store) into environment variables — rss2msg expands `${VAR}` in
the config and honors `RSS2MSG_`-prefixed overrides.

```json
{
  "family": "rss2msg",
  "requiresCompatibilities": ["FARGATE"],
  "networkMode": "awsvpc",
  "cpu": "256",
  "memory": "512",
  "taskRoleArn": "arn:aws:iam::123456789012:role/rss2msg-task",
  "executionRoleArn": "arn:aws:iam::123456789012:role/rss2msg-exec",
  "containerDefinitions": [
    {
      "name": "rss2msg",
      "image": "123456789012.dkr.ecr.us-east-1.amazonaws.com/rss2msg:latest",
      "command": ["serve"],
      "portMappings": [
        { "name": "health", "containerPort": 8080, "protocol": "tcp" },
        { "name": "metrics", "containerPort": 9090, "protocol": "tcp" }
      ],
      "secrets": [
        {
          "name": "POSTGRES_DSN",
          "valueFrom": "arn:aws:secretsmanager:us-east-1:123456789012:secret:rss2msg/postgres-dsn"
        }
      ],
      "logConfiguration": {
        "logDriver": "awslogs",
        "options": {
          "awslogs-group": "/ecs/rss2msg",
          "awslogs-region": "us-east-1",
          "awslogs-stream-prefix": "rss2msg"
        }
      }
    }
  ]
}
```

Run it as an ECS **service** with `desiredCount: 1`.

## Health checks

The image is distroless with no shell, so an ECS container-level `healthCheck`
(which runs a command inside the container) can't work. Instead:

- If the service sits behind an Application Load Balancer, set the target group
  health check to `GET /readyz` on port `8080`.
- Otherwise omit the check — ECS restarts the task when the process exits, and
  `serve` exits non-zero on fatal errors.

See [Kubernetes Health Probes](../kubernetes-health-probes.md) for the meaning of
`/healthz`, `/readyz`, and `/startupz`.

## IAM for AWS sinks

The SQS and SNS sinks use the default AWS credential chain, so they pick up the ECS
**task role** automatically — no static keys in config. Grant the task role the
relevant `sqs:SendMessage` / `sns:Publish` permissions. See
[Operational Notes](../../explanation/operations.md) for the credential chain and
[SQS](../sinks/sqs.md) / [SNS](../sinks/sns.md) for sink config.

## Scheduled runs

For a cron-style job instead of a daemon, register a task definition whose command
is `["run-once"]` and trigger it with **EventBridge Scheduler → ECS RunTask**.

## Scaling

There is no leader election: extra `serve` tasks without a shared coordinator each
poll every feed. Keep `desiredCount: 1`, or point all tasks at a shared
Postgres/Redis coordinator — see [Run Multiple Instances](../run-multiple-instances.md).

## Related

- [Deploy in Production](../deploy.md) — config resolution, secrets, observability.
- [Operational Notes](../../explanation/operations.md) — AWS credential chain, delivery semantics.
- [Run Multiple Instances](../run-multiple-instances.md) — shared coordinator setup.
- [SQS](../sinks/sqs.md) / [SNS](../sinks/sns.md) — AWS sink configuration.
- [CLI](../../reference/cli.md) — `serve`, `run-once`, `validate-config`.
