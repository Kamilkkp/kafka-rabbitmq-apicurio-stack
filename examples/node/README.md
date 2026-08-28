# Node.js CDC consumer

Standalone NestJS example that consumes Debezium Avro CDC from this stack.
Steady-state delivery is RabbitMQ. Kafka is used only for an optional startup
replay. There is no offset-based deduplication: replay overlap with the bridge
is processed twice, so handlers must be idempotent.

## Prerequisites

From the repository root, start the demo stack:

```bash
cp .env.demo.example .env
docker compose -f compose.yaml -f compose.demo.yaml up --build -d
```

Host endpoints this consumer uses:

- Kafka: `localhost:9092`
- Apicurio (ccompat): `http://localhost:8081`
- RabbitMQ AMQP: `localhost:5672` (`admin` / `admin` in the demo env)

`CDC_SALES_TOPIC`, `CDC_WAREHOUSE_TOPIC`, and `CDC_RABBITMQ_EXCHANGE` must
match the stack (`sales.cdc`, `warehouse.cdc`, `cdc.events`).

RabbitMQ routing keys are `{database}.{schema}.{table}`, for example
`sales.public.customers` and `warehouse.public.products`. Avro subject lookup
uses the Kafka topic from the `kafka-topic` AMQP header (with a `sales.*` /
`warehouse.*` fallback).

## Run

```bash
cd examples/node
cp .env.example .env
npm install
npm start
```

`npm start` loads `.env` via `tsx --env-file=.env`. Watch mode: `npm run start:watch`.

Ongoing delivery is RabbitMQ-only. To replay the compacted Kafka topics from
offset 0 once at startup, then switch to RabbitMQ, set `CDC_KAFKA_REPLAY=true`
in `.env`. Replay does not skip events already seen on RabbitMQ.

Use a distinct `CDC_KAFKA_GROUP_ID` and `CDC_RABBITMQ_QUEUE` for every
independent projection.

## Config

| Variable | Role |
| --- | --- |
| `CDC_KAFKA_BROKERS` | Kafka bootstrap (`localhost:9092`) |
| `CDC_KAFKA_GROUP_ID` | Consumer group used only during Kafka replay |
| `CDC_APICURIO_URL` | Registry base URL (`http://localhost:8081`) |
| `CDC_SALES_TOPIC` | Compacted sales CDC topic (Avro subject key) |
| `CDC_WAREHOUSE_TOPIC` | Compacted warehouse CDC topic |
| `CDC_RABBITMQ_URL` | AMQP URL |
| `CDC_RABBITMQ_EXCHANGE` | Topic exchange declared by the stack |
| `CDC_RABBITMQ_QUEUE` | Queue name prefix; each source binds `<prefix>.<db.schema.table>` |
| `CDC_RABBITMQ_PREFETCH` | RabbitMQ prefetch |
| `CDC_KAFKA_REPLAY` | `true` to replay Kafka from offset 0 before RabbitMQ |

Null Kafka values are tombstones and are handled without Avro decoding. The
client talks to Apicurio over `ccompat` with `subjectNameStrategyType: TOPIC`.
Recreating a Kafka topic invalidates its internal id; restart this consumer
(and the bridge) if replay loops on `UNKNOWN_TOPIC_ID`.
