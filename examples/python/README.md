# Python CDC consumer (PoC)

Standalone proof of concept that **reads Debezium Avro events from Kafka**
and decodes them via Confluent Schema Registry.

Unlike the Node example, this PoC does not add a service-owned durable job
queue. It decodes and handles records directly in the Kafka polling loop.

## Stack endpoints (localhost)

Start the demo stack from the repository root:

```bash
cp .env.demo.example .env
docker compose -f compose.yaml -f compose.demo.yaml up --build -d
```

This consumer talks to:

| Service  | URL |
|----------|-----|
| Kafka            | `localhost:9092` (`SASL_PLAINTEXT` / `SCRAM-SHA-512`) |
| Schema Registry  | `http://localhost:8081` |

Default compacted CDC topics are one per table, for example
`sales.public.customers` and `warehouse.public.products`.

## Setup

```bash
cd examples/python
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
cp .env.example .env
```

Adjust `KAFKA_GROUP_ID` if another consumer already uses the same group.

## Run

```bash
set -a && source .env && set +a
.venv/bin/python cdc_consumer.py
```

One-shot smoke (pull up to N messages, then exit):

```bash
ONESHOT=1 ONESHOT_MAX=5 .venv/bin/python cdc_consumer.py
```

To re-read topics from the beginning on assign:

```bash
SEEK_TO_BEGINNING=sales.public.customers,warehouse.public.products .venv/bin/python cdc_consumer.py
```

## Config

These are every variable the script reads. See `.env.example` for values that
work against the local stack.

These have no fallback — if one is missing or empty, startup exits with
`Missing env <name>`. SASL username and password are required unless
`KAFKA_SECURITY_PROTOCOL=PLAINTEXT`:

| Variable | Notes |
|----------|-------|
| `KAFKA_BROKERS` | Bootstrap servers passed to librdkafka |
| `KAFKA_GROUP_ID` | Consumer group; prefix `example-` or `cdc-consumer` on this stack |
| `KAFKA_SASL_USERNAME` | Kafka principal (`cdc-consumer`) when protocol is not `PLAINTEXT` |
| `KAFKA_SASL_PASSWORD` | Kafka password when protocol is not `PLAINTEXT` |
| `SCHEMA_REGISTRY_URL` | Schema Registry URL (`http://localhost:8081`) |

The rest are optional:

| Variable | Default | Notes |
|----------|---------|-------|
| `KAFKA_SECURITY_PROTOCOL` | `SASL_PLAINTEXT` | `PLAINTEXT` disables SASL |
| `KAFKA_SASL_MECHANISM` | `SCRAM-SHA-512` | Used when the protocol is SASL |
| `KAFKA_TOPICS` | the six demo table topics | Comma-separated topic names to subscribe to |
| `KAFKA_TOPIC_PATTERN` | unset | Takes precedence over `KAFKA_TOPICS`. **Must start with `^`**, otherwise librdkafka reads it as a literal topic name rather than a regex |
| `SEEK_TO_BEGINNING` | unset | Comma-separated topics rewound to offset 0 on partition assign |
| `ONESHOT` | unset | Exactly `1` enables one-shot mode; any other value is ignored |
| `ONESHOT_MAX` | `5` | One-shot only: stop after this many messages (it also gives up after 30 empty polls) |

The six default topics are `sales.public.{customers,orders,order_items}` and
`warehouse.public.{products,warehouses,stock_levels}`. Debezium creates a topic
per captured table, so a table added later needs either `KAFKA_TOPICS` or a
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
