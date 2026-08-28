# Node.js CDC consumer

Standalone NestJS example that consumes Debezium Avro CDC through RabbitMQ.
Kafka is used only by an optional startup replay publisher, which republishes
the original Avro bytes to the same RabbitMQ exchange.

## Apply pattern (the part to copy)

`src/apply/` is the reference projection, independent of Nest:

The single rule is last-write-wins per `(table, pk)` on Debezium `source.lsn`.
Nothing is buffered and deletes always execute where they appear.

Handlers run **inside** the gate, and the watermark advances only after they
return. The watermark therefore records applied writes, not seen messages: a
handler that throws leaves it untouched, the message is nacked, and the
redelivery is not treated as stale. A child row whose parent has not arrived
yet can simply fail and be retried until the parent's own queue catches up —
no deferred foreign keys required, because every table has its own queue and
they drain independently.

Nothing requires messages to be processed in order, so prefetch is not pinned
to 1. Because the write sits between reading the watermark and moving it, the
engine serializes work per `(table, pk)`; rows are independent, so concurrency
is preserved. On SQL you get this for free from a single conditional upsert
(`... WHERE excluded.lsn > projection.lsn`) or a row lock held for the
transaction. Replay end markers may therefore overtake events still in flight,
which is why they are logging only.

Failures do not requeue onto the working queue. The error handler confirm-
publishes the original bytes and properties into a durable delay queue, then
acknowledges the original; if that publish fails, only then does it requeue the
original so the event cannot be lost. Queue TTLs produce the backoff
`5s → 30s → 5m → 30m`, after which RabbitMQ dead-letters the message back to
the original exchange and table routing key. It is parked in a durable,
unconsumed `<prefix>.dlq.<source>` queue after 100 failed deliveries or three
days since the first failure, whichever comes first. A working-queue message
TTL is deliberately not used: that would expire a genuine backlog as happily
as a poison message.

| Mode | When | Stale event (`source.lsn`) |
| --- | --- | --- |
| `replay` | Kafka → RabbitMQ with `cdc-mode=replay` | `< watermark` skipped; `= watermark` deliberately reapplied |
| `live` | regular bridge → RabbitMQ | `<= watermark` skipped |

Events are applied one at a time, exactly as RabbitMQ delivers them. A delete
that passes the LWW gate is applied and cascades — in the example, into
`ReadModel.follows`, a stand-in for local user progress that is never in CDC.
Consequence worth knowing: if the log still holds an intermediate delete for a
row that a later event re-inserts, replay executes that delete, and the
cascaded local row does not come back when the insert lands.

`npm test` covers these cases without Kafka.

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

Kafka topics and RabbitMQ routing keys are `{database}.{schema}.{table}`,
for example `sales.public.customers`. Avro subject lookup uses that same
name (`<topic>-value`).

## Run

```bash
cd examples/node
cp .env.example .env
npm install
npm start
```

`npm start` loads `.env` via `tsx --env-file=.env`. Watch mode: `npm run start:watch`.

Ongoing delivery is RabbitMQ-only. Set `CDC_KAFKA_REPLAY=true` to read the
compacted Kafka table topics up to their startup high watermarks and publish
them back to RabbitMQ with:

- `cdc-mode: replay`
- `cdc-replay-id: <uuid>`
- original Kafka topic, partition and offset
- one `cdc-replay-end` marker per table queue, so `ReplayCoordinator` can log
  when the replay has been fully consumed (reporting only, nothing waits on it)

The application still compares Debezium `source.lsn`. During replay an event
equal to the persisted watermark is intentionally executed again (projection
rebuild); an event below it is stale and skipped. In live mode equality means
redelivery and is skipped.

Use a distinct `CDC_KAFKA_GROUP_ID` and `CDC_RABBITMQ_QUEUE` for every
independent projection.

## Config

| Variable | Role |
| --- | --- |
| `CDC_KAFKA_BROKERS` | Kafka bootstrap (`localhost:9092`) |
| `CDC_KAFKA_GROUP_ID` | Consumer group used only during Kafka replay |
| `CDC_APICURIO_URL` | Registry base URL (`http://localhost:8081`) |
| `CDC_TOPICS` | Optional explicit replay topics; default lists the broker and drops excluded ones |
| `CDC_TOPIC_EXCLUDE` | Optional comma-separated regexes replacing the default infrastructure deny list |
| `CDC_RABBITMQ_URL` | AMQP URL |
| `CDC_RABBITMQ_EXCHANGE` | Topic exchange declared by the stack |
| `CDC_RABBITMQ_QUEUE` | Queue name prefix; each source binds `<prefix>.<db.schema.table>` |
| `CDC_RABBITMQ_PREFETCH` | Unacked messages in flight per consumer; defaults to `20`. Set `1` to process strictly in order |
| `CDC_RABBITMQ_RETRY_MAX_ATTEMPTS` | Failed deliveries before parking; defaults to `100` |
| `CDC_RABBITMQ_RETRY_MAX_AGE_MS` | Time since first failure before parking; defaults to `259200000` (3 days) |
| `CDC_KAFKA_REPLAY` | `true` to replay Kafka from offset 0 before RabbitMQ |

Null Kafka values are tombstones and are handled without Avro decoding. The
client talks to Apicurio over `ccompat` with `subjectNameStrategyType: TOPIC`.
Recreating a Kafka topic invalidates its internal id; restart this consumer
(and the bridge) if replay loops on `UNKNOWN_TOPIC_ID`.
