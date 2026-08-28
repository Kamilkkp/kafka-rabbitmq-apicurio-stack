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

Default compacted CDC topics: `sales.cdc` and `warehouse.cdc`.

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
CDC_SEEK_TO_BEGINNING=sales.cdc,warehouse.cdc .venv/bin/python cdc_consumer.py
```

## Config

| Variable | Required | Default / notes |
|----------|----------|-----------------|
| `CDC_KAFKA_BROKERS` | yes | `localhost:9092` |
| `CDC_KAFKA_GROUP_ID` | yes | consumer group |
| `CDC_APICURIO_URL` | yes | registry origin; client appends `/apis/ccompat/v7` |
| `CDC_SALES_TOPIC` | no | `sales.cdc` |
| `CDC_WAREHOUSE_TOPIC` | no | `warehouse.cdc` |
| `CDC_TOPICS` | no | comma-separated override of both topics |
| `CDC_KAFKA_TOPIC_PATTERN` | no | if set, used instead of the topic list (Kafka regex subscribe) |
| `CDC_SEEK_TO_BEGINNING` | no | comma-separated topic names to seek to offset 0 |
| `CDC_ONESHOT` | no | `1` = drain then exit |
| `CDC_ONESHOT_MAX` | no | `5` when oneshot |

`auto.offset.reset` is `earliest`; offsets are committed only after a
successful handle (or tombstone). Unhandled events fail the message and
do not commit.

Decoding uses official `confluent-kafka` `AvroDeserializer` (fastavro).
`python-schema-registry-client` cannot resolve Confluent schema
`references` and will fail on the Debezium envelope.

Typed sample handlers cover `sales.public.customers` and
`warehouse.public.products`.
Other tables are logged by the generic handler.
