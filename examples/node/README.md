# Node.js CDC consumer

NestJS example with a service-owned ingestion path:

```text
Kafka + Schema Registry
  -> generic Avro decode
  -> PostgreSQL / pg-boss
  -> table-specific Zod handler
  -> LWW projection write
```

RabbitMQ is not involved. Kafka is the shared CDC contract; pg-boss is local
to this consumer and uses the consumer's own PostgreSQL database.

## Why enqueue before Zod

The Kafka callback only:

1. decodes the Avro envelope and key through Schema Registry;
2. converts Avro `bigint` values to JSON-safe decimal strings;
3. inserts a pg-boss job;
4. commits the next Kafka offset.

No table-specific Zod schema runs before the job is durable. An incompatible
application schema therefore retries in pg-boss and can eventually reach the
service's DLQ without stopping the Kafka partition.

For live traffic the pg-boss job ID is a deterministic UUID derived from
`topic/partition/offset`. If the process dies after the PostgreSQL commit but
before the Kafka commit, Kafka redelivers the record and the duplicate job
insert becomes a no-op. An explicit replay adds its run ID to that UUID input:
duplicates within one run still collapse, while a later replay can deliberately
execute `LSN == watermark` again.

## Processing and retries

The queue uses pg-boss `key_strict_fifo` with a singleton key built from the
Kafka topic and decoded Debezium key:

- jobs for different rows run concurrently;
- jobs for one row remain ordered;
- a failed child row does not block its parent or unrelated rows;
- retry begins at 5 seconds, uses exponential backoff, and is capped at
  30 minutes;
- after 100 retries the job moves to `cdc-events-dead-letter`;
- DLQ jobs are retained in PostgreSQL for manual inspection and redrive.

`src/apply/` demonstrates LWW per `(table, primary key)` on Debezium
`source.lsn`. Live delivery requires `LSN > watermark`; replay also accepts
equality so the latest known event is deliberately applied again. The
watermark advances only after the handler succeeds.

The demo read model and watermark store are in memory to keep the sample
focused. A production implementation must store the projection and watermark
atomically in its application database, for example with a conditional upsert
or row lock in one transaction.

## Prerequisites

From the repository root:

```bash
cp .env.demo.example .env
docker compose -f compose.yaml -f compose.demo.yaml up --build -d
```

The consumer uses:

- Kafka: `localhost:9092`
- Schema Registry: `http://localhost:8081`
- service-owned PostgreSQL: `localhost:5435`, database `consumer`

## Run

```bash
cd examples/node
cp .env.example .env
npm install
npm start
```

`npm start` loads `.env` through `tsx --env-file=.env`.

Normal operation resumes the offsets stored for `CDC_KAFKA_GROUP_ID`. Set
`CDC_KAFKA_REPLAY=true` for a one-shot startup replay from offset zero to the
captured high watermarks; replay jobs are all enqueued before the live consumer
starts.

Use a unique `CDC_KAFKA_GROUP_ID` and PostgreSQL database/schema for every
independent projection.

## Configuration

| Variable | Role |
| --- | --- |
| `CDC_KAFKA_BROKERS` | Kafka bootstrap (`localhost:9092`) |
| `CDC_KAFKA_GROUP_ID` | This projection's persistent live consumer group |
| `CDC_KAFKA_CONCURRENCY` | Kafka partitions enqueued concurrently; default `4` |
| `CDC_SCHEMA_REGISTRY_URL` | Schema Registry URL (`http://localhost:8081`) |
| `CDC_TOPICS` | Optional explicit replay topics; default is the registered processors |
| `CDC_KAFKA_REPLAY` | `true` to enqueue offset-zero replay before live consumption |
| `CDC_JOB_DATABASE_URL` | PostgreSQL connection used by pg-boss |
| `CDC_JOB_CONCURRENCY` | Local pg-boss worker concurrency; default `20` |
| `CDC_JOB_RETRY_LIMIT` | Retries before local DLQ; default `100` |
| `CDC_JOB_RETRY_DELAY_SECONDS` | Initial retry delay; default `5` |
| `CDC_JOB_RETRY_DELAY_MAX_SECONDS` | Backoff cap; default `1800` |

Kafka null values are tombstones and are committed without creating a job.
Recreating a Kafka topic invalidates librdkafka's internal topic ID; restart
the consumer if it reports `UNKNOWN_TOPIC_ID`.

Run unit tests:

```bash
npm test
```
