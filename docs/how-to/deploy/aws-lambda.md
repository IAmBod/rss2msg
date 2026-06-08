---
title: Deploy on AWS Lambda
type: how-to
tags: [rss2msg/docs, operations, deployment, aws]
summary: Run rss2msg as a native AWS Lambda function with the built-in lambda subcommand — container or zip packaging, DynamoDB state and coordination, an SNS sink, and CDK, SAM, or CLI deployment.
updated: 2026-06-08
---

# Deploy on AWS Lambda

rss2msg has a native Lambda entry point: the **`lambda`** subcommand. It does the
expensive wiring (config, state store, coordinator, sink connections) once at cold
start, then runs one poll-detect-publish cycle per invocation — the same work as
`run-once`, but driven by the Lambda Runtime API instead of a process exit. Trigger
it on a schedule and you get serverless, pay-per-run polling.

For the shared config, secrets, and observability model, see
[Deploy in Production](../deploy.md).

> If you just want a scheduled container and don't specifically need Lambda, the
> simpler path is EventBridge Scheduler → **ECS RunTask** with `run-once` — see
> [AWS ECS (Fargate)](aws-ecs.md). Lambda adds the constraints below.

## How the `lambda` subcommand works

```bash
rss2msg lambda
```

`lambda.Start` connects to the Lambda Runtime API and blocks, serving one
invocation at a time. Each invocation polls every configured feed once (bounded by
`runtime.run_once_concurrency`) and returns a small JSON summary
(`{"feeds":N,"ok":true}`); a poll failure is returned to the runtime so the
invocation is marked failed and retried per the trigger's policy. Cold-start
connections are reused across warm invocations.

The binary also **auto-starts the handler**: when it runs inside Lambda
(`AWS_LAMBDA_RUNTIME_API` is set) with no explicit subcommand, it behaves as if
`lambda` were passed. That lets a bare binary — a zip custom-runtime `bootstrap`,
or a container image with no command — start the handler with no wrapper, while
`rss2msg serve`/`run-once`/etc. still work when given explicitly.

The handler resolves the feed list the same way `run-once` does: the static
`feeds:` block **plus** any [`feed_sources`](../dynamic-feed-sources.md) (file,
Postgres), read once at cold start. Each cold start re-reads dynamic sources, so a
Postgres-backed feed table picks up adds and removals between invocations. If a
source is unreachable the invocation fails (and is retried) rather than polling a
partial set.

## Constraints to plan around

- **Ephemeral filesystem.** Lambda's disk is read-only except a temporary `/tmp`,
  so the default SQLite state store won't survive between invocations — change
  detection would reset every run and re-publish everything. Use an **external
  state store** (DynamoDB or Postgres).
- **Concurrent invocations.** Overlapping or scaled-out invocations must not
  double-poll a feed, so use a **distributed coordinator** (DynamoDB, Redis, or
  Postgres), never the in-process `memory` coordinator. The validator warns if you
  pair a distributed deployment with local-only backends.
- **15-minute cap.** A single invocation must finish within the function timeout
  (Lambda's max is 15 minutes). Keep the feed set per function bounded.

The recipes below use DynamoDB for both state and coordination, so the deployment
is fully serverless with no database to run.

## Configuration

`${VAR}` placeholders are expanded from the function's environment at load time, so
the IaC injects table names, the topic ARN, and any secrets without rebuilding the
package. Supply this `config.yaml` either baked into the image (container) or beside
the binary at `/var/task/config.yaml` with `--config` (zip):

```yaml
log:
  level: info
  format: json # CloudWatch Logs parses JSON into queryable fields

state:
  driver: dynamodb
  dynamodb:
    table: ${STATE_TABLE}
    # Region is left to the SDK default chain; Lambda sets AWS_REGION for us.
    ttl_attribute: expires_at # auto-prune old item rows (matches the table TTL)
    item_ttl: 720h            # 30 days

coordination:
  driver: dynamodb
  dynamodb:
    table: ${COORD_TABLE}
    lease_duration: 60s # must exceed worst-case per-feed poll time

runtime:
  run_once_concurrency: 8 # 0 lets rss2msg pick min(8, len(feeds))

sinks:
  - name: out
    driver: sns
    sns:
      topic_arn: ${SNS_TOPIC_ARN}

feeds:
  - url: https://hnrss.org/frontpage
    interval: 15m
    sinks: [out]
  - url: https://www.theverge.com/rss/index.xml
    interval: 15m
    sinks: [out]
```

## Packaging: container or zip

Lambda accepts either a container image or a zip on the `provided.al2023` custom
runtime. Both run the same binary; pick by your toolchain.

### Option A — container image

The binary speaks the Runtime API itself, so the image is just the binary on the
custom-runtime base — no shell bootstrap. This `Dockerfile.lambda` compiles from
source so it builds standalone:

```dockerfile
# syntax=docker/dockerfile:1

FROM golang:1.25-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
ARG TARGETARCH
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH} go build -trimpath -ldflags="-s -w" \
    -o /out/rss2msg ./cmd/rss2msg

FROM public.ecr.aws/lambda/provided:al2023
COPY --from=build /out/rss2msg /usr/local/bin/rss2msg
COPY config.yaml /etc/rss2msg/config.yaml   # a default search path
ENTRYPOINT ["/usr/local/bin/rss2msg", "lambda"]
```

To skip the compile and layer the published binary instead, replace the build stage
with `COPY --from=ghcr.io/iambod/rss2msg:latest /usr/local/bin/rss2msg
/usr/local/bin/rss2msg`.

### Option B — zip package (no container, no ECR)

Cross-compile a static binary named `bootstrap` and zip it with the config. Because
the binary auto-starts the handler inside Lambda, no wrapper script is needed:

```bash
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -trimpath -ldflags="-s -w" \
  -o bootstrap ./cmd/rss2msg
zip rss2msg-lambda.zip bootstrap config.yaml
```

The zip lands `config.yaml` at `/var/task/config.yaml`, which is not a default
search path, so point the handler at it with `--config`. Since the binary
auto-injects `lambda`, pass the flag through a tiny `bootstrap` wrapper **or** set
the function's command/args — the simplest is a one-line wrapper:

```sh
#!/bin/sh
exec /var/task/rss2msg.bin lambda --config /var/task/config.yaml
```

If your feed/sink config needs no `--config` (e.g. everything via `RSS2MSG_*` env),
skip the wrapper entirely: name the binary `bootstrap` and it self-starts.

## Deploying

### With AWS CDK (container image)

The CDK stack provisions the two DynamoDB tables (with the exact key schema the
DynamoDB state and coordinator expect), an SNS topic, the container function, an
EventBridge schedule, and least-privilege IAM. Scaffold a TypeScript CDK app
(`cdk init app --language typescript`), drop `Dockerfile.lambda` and `config.yaml`
beside it, and use this stack:

```typescript
import * as path from 'path';
import { Duration, RemovalPolicy, Stack, StackProps, CfnOutput } from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as sns from 'aws-cdk-lib/aws-sns';
import * as events from 'aws-cdk-lib/aws-events';
import * as targets from 'aws-cdk-lib/aws-events-targets';
import { Platform } from 'aws-cdk-lib/aws-ecr-assets';

export class Rss2msgLambdaStack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    // State store: composite key (feed_url, item_id); TTL matches the config.
    const stateTable = new dynamodb.Table(this, 'StateTable', {
      partitionKey: { name: 'feed_url', type: dynamodb.AttributeType.STRING },
      sortKey: { name: 'item_id', type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      timeToLiveAttribute: 'expires_at',
      removalPolicy: RemovalPolicy.DESTROY, // PoC convenience; revisit for prod
    });

    // Coordinator lock table: single partition key "pk". Lease liveness uses a
    // conditional-expiry check, not native TTL, so no TTL attribute is needed.
    const coordTable = new dynamodb.Table(this, 'CoordTable', {
      partitionKey: { name: 'pk', type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      removalPolicy: RemovalPolicy.DESTROY,
    });

    const topic = new sns.Topic(this, 'OutputTopic');

    const fn = new lambda.DockerImageFunction(this, 'Poller', {
      code: lambda.DockerImageCode.fromImageAsset(path.join(__dirname, '..'), {
        file: 'Dockerfile.lambda',
        platform: Platform.LINUX_ARM64,
      }),
      architecture: lambda.Architecture.ARM_64,
      memorySize: 512,
      timeout: Duration.minutes(5), // must cover the slowest feed + sink fan-out
      // One scheduled invocation at a time. The DynamoDB coordinator is the real
      // double-poll guard; this just keeps the PoC's cost and log noise low.
      reservedConcurrentExecutions: 1,
      environment: {
        STATE_TABLE: stateTable.tableName,
        COORD_TABLE: coordTable.tableName,
        SNS_TOPIC_ARN: topic.topicArn,
      },
    });

    // Least-privilege grants scoped to exactly these resources.
    stateTable.grantReadWriteData(fn);
    coordTable.grantReadWriteData(fn);
    topic.grantPublish(fn);

    new events.Rule(this, 'PollSchedule', {
      schedule: events.Schedule.rate(Duration.minutes(15)),
      targets: [new targets.LambdaFunction(fn)],
    });

    new CfnOutput(this, 'FunctionName', { value: fn.functionName });
    new CfnOutput(this, 'OutputTopicArn', { value: topic.topicArn });
  }
}
```

Deploy it — CDK builds and pushes the image asset for you:

```bash
npm install
cdk bootstrap   # first time in this account/region
cdk deploy
```

### With AWS SAM

SAM is a good fit if you prefer CloudFormation YAML. This `template.yaml` builds the
same container image (`Metadata.DockerContext` + `Dockerfile`) and schedules it; the
two DynamoDB tables and SNS topic are declared alongside, with table/topic names
passed in as environment variables:

```yaml
AWSTemplateFormatVersion: '2010-09-09'
Transform: AWS::Serverless-2016-10-31

Resources:
  StateTable:
    Type: AWS::DynamoDB::Table
    Properties:
      BillingMode: PAY_PER_REQUEST
      AttributeDefinitions:
        - { AttributeName: feed_url, AttributeType: S }
        - { AttributeName: item_id, AttributeType: S }
      KeySchema:
        - { AttributeName: feed_url, KeyType: HASH }
        - { AttributeName: item_id, KeyType: RANGE }
      TimeToLiveSpecification: { AttributeName: expires_at, Enabled: true }

  CoordTable:
    Type: AWS::DynamoDB::Table
    Properties:
      BillingMode: PAY_PER_REQUEST
      AttributeDefinitions:
        - { AttributeName: pk, AttributeType: S }
      KeySchema:
        - { AttributeName: pk, KeyType: HASH }

  OutputTopic:
    Type: AWS::SNS::Topic

  Poller:
    Type: AWS::Serverless::Function
    Metadata:
      Dockerfile: Dockerfile.lambda
      DockerContext: .
    Properties:
      PackageType: Image
      Architectures: [arm64]
      MemorySize: 512
      Timeout: 300
      ReservedConcurrentExecutions: 1
      Environment:
        Variables:
          STATE_TABLE: !Ref StateTable
          COORD_TABLE: !Ref CoordTable
          SNS_TOPIC_ARN: !Ref OutputTopic
      Policies:
        - DynamoDBCrudPolicy: { TableName: !Ref StateTable }
        - DynamoDBCrudPolicy: { TableName: !Ref CoordTable }
        - SNSPublishMessagePolicy: { TopicName: !GetAtt OutputTopic.TopicName }
      Events:
        Schedule:
          Type: Schedule
          Properties:
            Schedule: rate(15 minutes)
```

```bash
sam build && sam deploy --guided
```

For a **zip** SAM function instead, drop the `Metadata` block, set
`Runtime: provided.al2023`, `Handler: bootstrap`, and `CodeUri:` to the directory
holding the `bootstrap` binary and `config.yaml`.

### With the AWS CLI (zip)

No IaC at all — create the tables, role, function, and schedule directly. Build the
zip as in Option B, then:

```bash
aws dynamodb create-table --table-name rss2msg-state \
  --billing-mode PAY_PER_REQUEST \
  --attribute-definitions AttributeName=feed_url,AttributeType=S AttributeName=item_id,AttributeType=S \
  --key-schema AttributeName=feed_url,KeyType=HASH AttributeName=item_id,KeyType=RANGE
aws dynamodb create-table --table-name rss2msg-coord \
  --billing-mode PAY_PER_REQUEST \
  --attribute-definitions AttributeName=pk,AttributeType=S \
  --key-schema AttributeName=pk,KeyType=HASH

aws lambda create-function \
  --function-name rss2msg \
  --runtime provided.al2023 --architectures arm64 --handler bootstrap \
  --zip-file fileb://rss2msg-lambda.zip \
  --role arn:aws:iam::<acct>:role/rss2msg-lambda \
  --timeout 300 --memory-size 512 \
  --environment "Variables={STATE_TABLE=rss2msg-state,COORD_TABLE=rss2msg-coord,SNS_TOPIC_ARN=arn:aws:sns:<region>:<acct>:rss2msg}"

aws scheduler create-schedule --name rss2msg-every-15m \
  --schedule-expression "rate(15 minutes)" \
  --target '{"Arn":"arn:aws:lambda:<region>:<acct>:function:rss2msg","RoleArn":"arn:aws:iam::<acct>:role/rss2msg-scheduler"}' \
  --flexible-time-window '{"Mode":"OFF"}'
```

EventBridge Scheduler (above) gives timezone-aware cron and one-shot schedules; a
plain EventBridge **rule** works too if you prefer `rate()`/`cron()` without a
scheduler role.

## IAM for AWS sinks

The SNS, SQS, and DynamoDB sinks use the default AWS credential chain, so they pick
up the Lambda **execution role** automatically — no static keys in config. The CDK
grants and SAM policies above scope the role to exactly the tables and topic it
uses. If you switch sinks, grant the matching action (`sqs:SendMessage`, etc.). See
[Operational Notes](../../explanation/operations.md) for the credential chain and
[SQS](../sinks/sqs.md) / [SNS](../sinks/sns.md) for sink config.

## Related

- [AWS ECS (Fargate)](aws-ecs.md) — the long-lived daemon, and the simpler RunTask schedule.
- [Deploy in Production](../deploy.md) — config resolution, secrets, observability.
- [Run Multiple Instances](../run-multiple-instances.md) — coordinator setup and double-poll safety.
- [Operational Notes](../../explanation/operations.md) — AWS credential chain, delivery semantics.
- [SQS](../sinks/sqs.md) / [SNS](../sinks/sns.md) — AWS sink configuration.
- [CLI](../../reference/cli.md) — `lambda`, `run-once`, `validate-config`.
