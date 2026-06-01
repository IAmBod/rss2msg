---
title: Deploy on AWS Lambda (run-once)
type: how-to
tags: [rss2msg/docs, operations, deployment, aws]
summary: Run rss2msg run-once on AWS Lambda as a scheduled container function — a custom-runtime wrapper, Postgres state for persistence, secrets and IAM, and an EventBridge schedule.
updated: 2026-06-01
---

# Deploy on AWS Lambda (run-once)

Lambda fits the `run-once` model: each invocation polls every feed once and exits.
Trigger it on a schedule and you get serverless, pay-per-run polling. For the config,
secrets, and observability model, see [Deploy in Production](../deploy.md).

> If you just want a scheduled container and don't specifically need Lambda, the
> simpler path is EventBridge Scheduler → **ECS RunTask** with the `run-once`
> command — see [AWS ECS (Fargate)](aws-ecs.md). Lambda adds the constraints below.

## Constraints to plan around

- **No native handler.** rss2msg is a CLI, not a Lambda function; the published
  image's entrypoint doesn't speak the Lambda Runtime API. So you run it through a
  thin custom-runtime (`provided.al2023`) wrapper that invokes `run-once` per call.
- **Persistent state is required.** Lambda's filesystem is read-only except an
  ephemeral `/tmp`, so the default SQLite state store (a local `./rss2msg.db` file)
  won't survive between invocations — change detection would reset every run and
  re-publish everything. Use the **Postgres state store** so `run-once` remembers
  what it has already seen:

  ```yaml
  state:
    driver: postgres
    postgres:
      dsn: ${POSTGRES_DSN}
  ```

- **15-minute cap.** A single `run-once` must finish within the function timeout
  (Lambda's max is 15 minutes). Keep the feed set per function bounded.

## Build the Lambda image

Layer the prebuilt binary (it lives at `/usr/local/bin/rss2msg` in the published
image) and your config onto the AWS custom-runtime base, alongside a `bootstrap`
that runs `run-once` on each invocation:

```dockerfile
FROM public.ecr.aws/lambda/provided:al2023
RUN dnf install -y curl-minimal && dnf clean all   # the bootstrap needs an HTTP client

COPY --from=ghcr.io/iambod/rss2msg:latest /usr/local/bin/rss2msg /var/task/rss2msg
COPY config.yaml   /var/task/config.yaml
COPY bootstrap     /var/runtime/bootstrap
RUN chmod +x /var/runtime/bootstrap
```

`bootstrap` implements the minimal Runtime API loop — fetch an invocation, run
`run-once`, report success or failure:

```sh
#!/bin/sh
set -eu
API="http://${AWS_LAMBDA_RUNTIME_API}/2018-06-01/runtime/invocation"
while true; do
  HEADERS="$(mktemp)"
  curl -sS -LD "$HEADERS" "$API/next" >/dev/null
  REQ_ID="$(grep -Fi 'Lambda-Runtime-Aws-Request-Id' "$HEADERS" | tr -d '[:space:]' | cut -d: -f2)"
  if /var/task/rss2msg run-once --config /var/task/config.yaml; then
    curl -sS "$API/$REQ_ID/response" -d '{"status":"ok"}' >/dev/null
  else
    curl -sS "$API/$REQ_ID/error" -d '{"error":"run-once failed"}' >/dev/null
  fi
done
```

Keep secrets out of `config.yaml` — they arrive as Lambda environment variables and
are expanded via `${VAR}`. Build, push to ECR, and create the function from the image:

```bash
aws ecr create-repository --repository-name rss2msg-lambda
docker build -t rss2msg-lambda .
docker tag rss2msg-lambda:latest <acct>.dkr.ecr.<region>.amazonaws.com/rss2msg-lambda:latest
docker push <acct>.dkr.ecr.<region>.amazonaws.com/rss2msg-lambda:latest

aws lambda create-function \
  --function-name rss2msg-run-once \
  --package-type Image \
  --code ImageUri=<acct>.dkr.ecr.<region>.amazonaws.com/rss2msg-lambda:latest \
  --role arn:aws:iam::<acct>:role/rss2msg-lambda \
  --timeout 600 --memory-size 256 \
  --environment "Variables={POSTGRES_DSN=postgres://...}"
```

For real secrets, prefer a Secrets Manager lookup over an inline env value.

## Schedule it

Trigger the function on a cron with **EventBridge Scheduler** (or an EventBridge rule):

```bash
aws scheduler create-schedule \
  --name rss2msg-every-5m \
  --schedule-expression "rate(5 minutes)" \
  --target '{"Arn":"arn:aws:lambda:<region>:<acct>:function:rss2msg-run-once","RoleArn":"arn:aws:iam::<acct>:role/rss2msg-scheduler"}' \
  --flexible-time-window '{"Mode":"OFF"}'
```

## IAM for AWS sinks

The SQS and SNS sinks use the default AWS credential chain, so they pick up the
Lambda **execution role** automatically — no static keys in config. Grant that role
the relevant `sqs:SendMessage` / `sns:Publish` permissions. See
[Operational Notes](../../explanation/operations.md) for the credential chain and
[SQS](../sinks/sqs.md) / [SNS](../sinks/sns.md) for sink config.

## Related

- [AWS ECS (Fargate)](aws-ecs.md) — the long-lived daemon, and the simpler RunTask schedule.
- [Deploy in Production](../deploy.md) — config resolution, secrets, observability.
- [Operational Notes](../../explanation/operations.md) — AWS credential chain, delivery semantics.
- [SQS](../sinks/sqs.md) / [SNS](../sinks/sns.md) — AWS sink configuration.
- [CLI](../../reference/cli.md) — `run-once`, `validate-config`.
