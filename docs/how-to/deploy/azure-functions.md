---
title: Deploy on Azure Functions
type: how-to
tags: [rss2msg/docs, operations, deployment, azure]
summary: Run rss2msg as an Azure Functions custom handler with the built-in azure-functions subcommand — a timer trigger driving one poll cycle, Postgres state and coordination, an Azure Service Bus sink, and Core Tools, Azure CLI, or Bicep deployment.
updated: 2026-06-09
---

# Deploy on Azure Functions

rss2msg has a native Azure Functions entry point: the **`azure-functions`**
subcommand. It runs as a [custom handler](https://learn.microsoft.com/azure/azure-functions/functions-custom-handlers) —
a small HTTP server the Functions host invokes once per trigger firing. It does the
expensive wiring (config, state store, coordinator, sink connections) once at cold
start, then runs one poll-detect-publish cycle per request — the same work as
`run-once`. Bind it to a timer trigger and you get serverless, scheduled polling.

For the shared config, secrets, and observability model, see
[Deploy in Production](../deploy.md).

> If you just want a scheduled container and don't specifically need Functions, the
> simpler path is a Container Apps **job** running `run-once` — see
> [Azure Container Apps](azure-container-apps.md). Functions adds the constraints below.

## How the `azure-functions` subcommand works

```bash
rss2msg azure-functions
```

The handler listens on `127.0.0.1` at the port the Functions host provides via
`FUNCTIONS_CUSTOMHANDLER_PORT` (default `8080` when run outside the host). A single
catch-all route handles every function the app defines — the host POSTs to
`/<functionName>` on each trigger firing. Each request polls every configured feed
once (bounded by `runtime.run_once_concurrency`) and returns a small JSON summary
(`{"feeds":N,"ok":true}`); a poll failure is reported as HTTP 500 so the host marks
the invocation failed and retries per the trigger policy. Cold-start connections are
reused across warm invocations.

The binary also **auto-starts the handler**: when it runs inside the Functions host
(`FUNCTIONS_CUSTOMHANDLER_PORT` is set) with no explicit subcommand, it behaves as
if `azure-functions` were passed. That lets `defaultExecutablePath` point straight at
the binary with no arguments, while `rss2msg serve`/`run-once`/etc. still work when
given explicitly. (`rss2msg azure` is a shorter alias.)

The handler resolves the feed list the same way `run-once` does: the static
`feeds:` block **plus** any [`feed_sources`](../load-feeds-dynamically.md) (file,
Postgres), read once at cold start. Each cold start re-reads dynamic sources, so a
Postgres-backed feed table picks up adds and removals between cold starts. If a
source is unreachable the invocation fails (and is retried) rather than polling a
partial set.

## Constraints to plan around

- **Ephemeral filesystem.** A Functions worker's local disk does not persist across
  cold starts, so the default SQLite state store won't survive — change detection
  would reset and re-publish everything. Use an **external state store** (Postgres,
  e.g. Azure Database for PostgreSQL).
- **Concurrent executions.** The host scales workers out horizontally, so overlapping
  executions must not double-poll a feed. Use a **distributed coordinator** (Postgres
  or Redis, e.g. Azure Cache for Redis), never the in-process `memory` coordinator.
  The validator warns if you pair a distributed deployment with local-only backends.
- **Function timeout.** A single invocation must finish within the function timeout
  (the Consumption plan default is 5 minutes, max 10). Keep the feed set per function
  bounded.

The recipe below uses Postgres for state and coordination and an Azure Service Bus
sink, so the whole pipeline is Azure-native.

## Configuration

`${VAR}` placeholders are expanded from the app's environment (Function App
**application settings**) at load time, so infrastructure injects the DSN, the Service
Bus connection, and any secrets without rebuilding the package. The app's working
directory is the function app root, so a `config.yaml` placed there is found on the
default search path (`./config.yaml`) with no `--config` flag:

```yaml
log:
  level: info
  format: json # Application Insights / Log Analytics parses JSON into fields

state:
  driver: postgres
  postgres:
    dsn: ${POSTGRES_DSN}

coordination:
  driver: postgres
  postgres:
    dsn: ${POSTGRES_DSN}
    lease_duration: 60s # must exceed worst-case per-feed poll time

runtime:
  run_once_concurrency: 8 # 0 lets rss2msg pick min(8, len(feeds))

sinks:
  - name: out
    driver: azureservicebus
    azureservicebus:
      connection_string: ${SERVICEBUS_CONNECTION} # SAS auth; or set `namespace:`
      topic: rss-items                            # or `queue:` for a queue
    # Azure AD / managed identity instead of a connection string:
    # azureservicebus:
    #   namespace: my-ns.servicebus.windows.net   # uses DefaultAzureCredential
    #   topic: rss-items

feeds:
  - url: https://hnrss.org/frontpage
    interval: 15m
    sinks: [out]
  - url: https://www.theverge.com/rss/index.xml
    interval: 15m
    sinks: [out]
```

## Packaging: the function app layout

A custom handler is a function app whose worker is your binary. The app is a directory
with `host.json`, the Linux binary, the shared `config.yaml`, and one folder per
function holding its `function.json`:

```
rss2msg-func/
├── host.json
├── rss2msg          # the custom handler binary (linux/amd64)
├── config.yaml
└── poll/
    └── function.json
```

`host.json` registers the binary as the custom handler. Because the binary auto-starts
the handler inside the host, no `arguments` are needed (the explicit form is shown
commented for clarity):

```json
{
  "version": "2.0",
  "customHandler": {
    "description": {
      "defaultExecutablePath": "rss2msg"
    },
    "enableForwardingHttpRequest": false
  }
}
```

`poll/function.json` binds the function to a timer. The schedule is an
[NCRONTAB](https://learn.microsoft.com/azure/azure-functions/functions-bindings-timer)
expression (six fields, leading seconds) — `0 */15 * * * *` fires every 15 minutes:

```json
{
  "bindings": [
    {
      "name": "pollTimer",
      "type": "timerTrigger",
      "direction": "in",
      "schedule": "0 */15 * * * *"
    }
  ]
}
```

Build the worker as a static Linux binary beside `host.json`:

```bash
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" \
  -o rss2msg-func/rss2msg ./cmd/rss2msg
```

To skip the compile, pull the published binary out of the container image instead:
`docker create --name x ghcr.io/iambod/rss2msg:latest && docker cp
x:/usr/local/bin/rss2msg rss2msg-func/rss2msg && docker rm x`.

## Deploying

The timer trigger keeps its schedule state in the Function App's storage account, so
every option below provisions one. State, coordination, and the Service Bus topic are
expected to exist already (or be created alongside).

### With Azure Functions Core Tools

Create a custom-runtime Function App, set the application settings the config reads,
then publish the app folder with [`func`](https://learn.microsoft.com/azure/azure-functions/functions-run-local):

```bash
RG=rss2msg-rg
APP=rss2msg-func
SA=rss2msgstore$RANDOM
LOC=westeurope

az group create --name $RG --location $LOC
az storage account create --name $SA --resource-group $RG --location $LOC --sku Standard_LRS
az functionapp create --name $APP --resource-group $RG \
  --consumption-plan-location $LOC --os-type Linux \
  --runtime custom --functions-version 4 --storage-account $SA

# Application settings the config's ${VAR} placeholders resolve against.
az functionapp config appsettings set --name $APP --resource-group $RG --settings \
  POSTGRES_DSN="postgres://user:pass@host:5432/rss2msg?sslmode=require" \
  SERVICEBUS_CONNECTION="Endpoint=sb://my-ns.servicebus.windows.net/;..."

# Publish the app folder (host.json, the binary, config.yaml, poll/).
cd rss2msg-func && func azure functionapp publish $APP
```

### With the Azure CLI (zip deploy)

No Core Tools — zip the app folder and push it with `az`. Create the app exactly as
above, then:

```bash
(cd rss2msg-func && zip -r ../rss2msg-func.zip .)
az functionapp deployment source config-zip \
  --name $APP --resource-group $RG --src rss2msg-func.zip
```

### With Bicep

Declare the plan, storage, and Function App as infrastructure. This `main.bicep`
provisions a Linux Consumption app on the custom runtime and wires the same
application settings; deploy your app folder into it afterward with either method
above:

```bicep
param location string = resourceGroup().location
param appName string
@secure()
param postgresDsn string
@secure()
param serviceBusConnection string

resource storage 'Microsoft.Storage/storageAccounts@2023-01-01' = {
  name: toLower('${appName}sa')
  location: location
  sku: { name: 'Standard_LRS' }
  kind: 'StorageV2'
}

resource plan 'Microsoft.Web/serverfarms@2023-12-01' = {
  name: '${appName}-plan'
  location: location
  sku: { name: 'Y1', tier: 'Dynamic' } // Consumption
  properties: { reserved: true }       // Linux
}

resource app 'Microsoft.Web/sites@2023-12-01' = {
  name: appName
  location: location
  kind: 'functionapp,linux'
  identity: { type: 'SystemAssigned' } // for managed-identity Service Bus auth
  properties: {
    serverFarmId: plan.id
    reserved: true
    siteConfig: {
      appSettings: [
        { name: 'FUNCTIONS_EXTENSION_VERSION', value: '~4' }
        { name: 'FUNCTIONS_WORKER_RUNTIME', value: 'custom' }
        {
          name: 'AzureWebJobsStorage'
          value: 'DefaultEndpointsProtocol=https;AccountName=${storage.name};EndpointSuffix=${environment().suffixes.storage};AccountKey=${storage.listKeys().keys[0].value}'
        }
        { name: 'POSTGRES_DSN', value: postgresDsn }
        { name: 'SERVICEBUS_CONNECTION', value: serviceBusConnection }
      ]
    }
  }
}
```

```bash
az deployment group create --resource-group $RG \
  --template-file main.bicep \
  --parameters appName=$APP postgresDsn='...' serviceBusConnection='...'
```

## Managed identity for Azure sinks

The Azure Service Bus and Cosmos DB sinks support Azure AD auth via
`DefaultAzureCredential`, which picks up the Function App's **system-assigned managed
identity** (enabled by `identity` in the Bicep above) — no connection string in config.
Set `namespace:` (Service Bus) or `endpoint:` (Cosmos DB) instead of a connection
string, then grant the identity the matching data role (e.g. **Azure Service Bus Data
Sender**) on the resource. See [Azure Service Bus](../sinks/azureservicebus.md) and
[Azure Cosmos DB](../sinks/cosmosdb.md) for sink config.

## Related

- [Azure Container Apps](azure-container-apps.md) — the long-lived daemon, and the simpler scheduled job.
- [Deploy in Production](../deploy.md) — config resolution, secrets, observability.
- [Run Multiple Instances](../run-multiple-instances.md) — coordinator setup and double-poll safety.
- [Azure Service Bus](../sinks/azureservicebus.md) / [Azure Cosmos DB](../sinks/cosmosdb.md) — Azure sink configuration.
- [CLI](../../reference/cli.md) — `azure-functions`, `run-once`, `validate-config`.
