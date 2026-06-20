# RabbitMQ / AMQP integration

rss2msg publishing to **RabbitMQ** through the
[`amqp091` sink](../../../docs/how-to/sinks/amqp091.md) (AMQP 0-9-1). Messages are
routed to a topic exchange; a queue is pre-bound to that exchange so you can watch them
accumulate in the management UI.

## Run it

```bash
docker compose up
```

rss2msg waits for RabbitMQ's healthcheck before connecting. Once it polls the feed,
change envelopes are published to the `feed.changes` topic exchange with routing key
`feed.changes`, and land in the `feed.changes.q` queue.

Open the management UI at **http://localhost:15672** (user `guest`, password `guest`),
go to **Queues → feed.changes.q**, and use **Get messages** to inspect them.

Tear down with `docker compose down -v`.

## What's here

- **`rabbitmq`** — `rabbitmq:3.13-management`. Boots with a topology loaded from
  [`rabbitmq/definitions.json`](rabbitmq/definitions.json) (via
  [`rabbitmq/rabbitmq.conf`](rabbitmq/rabbitmq.conf)): the `feed.changes` topic
  exchange, the `feed.changes.q` queue, and a binding between them. Without a bound
  queue, messages published to an exchange are dropped — the binding is what makes the
  output observable.
- **`rss2msg`** — `amqp091` sink at `amqp://guest:guest@rabbitmq:5672/`, declaring the
  durable exchange at startup. See [`config.yaml`](config.yaml).

## Try changing it

- **Different routing** — switch `exchange_type` to `direct` or `fanout` and adjust the
  `routing_key`; update the binding in `definitions.json` to match.
- **TLS** — use an `amqps://` URL and add a `tls:` block to the sink (see
  [Secure connections](../../../docs/how-to/secure-connections-tls.md)).
- **Watch live** — bind a second queue and tail it, or use the management UI's message
  rates view.
