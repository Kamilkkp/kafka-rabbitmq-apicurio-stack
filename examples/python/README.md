# Python CDC consumer (PoC)

Standalone proof of concept that **reads Debezium Avro events from Kafka**
and decodes them via Apicurio’s Confluent-compatible registry API.

This example does **not** implement the Node consumer’s RabbitMQ handover
(no AMQP client, no queue bind, no Kafka replay then switch to RabbitMQ).
Live traffic on the stack is also bridged to RabbitMQ; this PoC ignores
that path and polls Kafka only.

Requires Debezium `artifact.group-id=default` so nested Envelope → Value /
Source schema references resolve.

## Stack endpoints (localhost)

Start the demo stack from the repository root:

```bash
cp .env.demo.example .env
docker compose -f compose.yaml -f compose.demo.yaml up --build -d
```

This consumer talks to:

| Service  | URL |
|----------|-----|
| Kafka    | `localhost:9092` |
| Apicurio | `http://localhost:8081` (`/apis/ccompat/v7`) |

Default compacted CDC topics are one per table, for example
`sales.public.customers` and `warehouse.public.products`.

## Setup

```bash
cd examples/python
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
cp .env.example .env
```

Adjust `CDC_KAFKA_GROUP_ID` if another consumer already uses the same group.

## Run

```bash
set -a && source .env && set +a
.venv/bin/python cdc_consumer.py
```

One-shot smoke (pull up to N messages, then exit):

```bash
CDC_ONESHOT=1 CDC_ONESHOT_MAX=5 .venv/bin/python cdc_consumer.py
```

To re-read topics from the beginning on assign:

```bash
CDC_SEEK_TO_BEGINNING=sales.public.customers,warehouse.public.products .venv/bin/python cdc_consumer.py
```

## Config

These are every variable the script reads. See `.env.example` for values that
work against the local stack.

Three are required and have no fallback — if one is missing or empty, startup
exits with `Missing env <name>`:

| Variable | Notes |
|----------|-------|
| `CDC_KAFKA_BROKERS` | Bootstrap servers passed to librdkafka |
| `CDC_KAFKA_GROUP_ID` | Consumer group; use a fresh one to re-read from the start |
| `CDC_APICURIO_URL` | Registry origin only; the client appends `/apis/ccompat/v7` |

The rest are optional:

| Variable | Default | Notes |
|----------|---------|-------|
| `CDC_TOPICS` | the six demo table topics | Comma-separated topic names to subscribe to |
| `CDC_KAFKA_TOPIC_PATTERN` | unset | Takes precedence over `CDC_TOPICS`. **Must start with `^`**, otherwise librdkafka reads it as a literal topic name rather than a regex |
| `CDC_SEEK_TO_BEGINNING` | unset | Comma-separated topics rewound to offset 0 on partition assign |
| `CDC_ONESHOT` | unset | Exactly `1` enables one-shot mode; any other value is ignored |
| `CDC_ONESHOT_MAX` | `5` | One-shot only: stop after this many messages (it also gives up after 30 empty polls) |

The six default topics are `sales.public.{customers,orders,order_items}` and
`warehouse.public.{products,warehouses,stock_levels}`. Debezium creates a topic
per captured table, so a table added later needs either `CDC_TOPICS` or a
pattern.

`auto.offset.reset` is `earliest`; offsets are committed only after a
successful handle (or tombstone). Unhandled events fail the message and
do not commit.

Decoding uses official `confluent-kafka` `AvroDeserializer` (fastavro).
`python-schema-registry-client` cannot resolve Confluent schema
`references` and will fail on the Debezium envelope.

Typed sample handlers cover `sales.public.customers` and
`warehouse.public.products`.
Other tables are logged by the generic handler.
