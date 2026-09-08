# Kafka, Debezium, and Confluent Schema Registry CDC stack

Standalone Docker Compose infrastructure that streams PostgreSQL changes:

```text
PostgreSQL (external, one database per connector)
  -> Debezium on Kafka Connect
  -> one compacted Kafka topic per table (`{prefix}.{schema}.{table}`)
  -> application-owned Kafka consumer
```

Confluent Schema Registry stores the Avro schemas and Kafbat UI exposes Kafka,
Kafka Connect, and the registry. There is intentionally no shared application
broker: Kafka is the CDC contract and every service owns its consumer group,
retry policy, dead-letter queue, and optional local job system.

[`compose.yaml`](compose.yaml) contains only shared CDC infrastructure. Point
its connectors at real PostgreSQL databases with environment variables.
[`compose.demo.yaml`](compose.demo.yaml) adds sample `sales` and `warehouse`
sources plus a service-owned PostgreSQL used by the Node pg-boss example.

## Repository layout

```text
.
├── compose.yaml
├── compose.demo.yaml
├── .env.example
├── .env.demo.example
├── docker/kafka-connect/
├── postgres/sales/
├── postgres/warehouse/
└── examples/
    ├── node/    # Kafka -> pg-boss -> LWW projection
    └── python/  # direct Kafka PoC
```

## Try it locally

You need Docker Compose v2 and free host ports `9092`, `8081`–`8083`, and
`5433`–`5435`.

```bash
cp .env.demo.example .env
docker compose -f compose.yaml -f compose.demo.yaml up --build -d
```

Wait for Kafka and both connectors:

```bash
docker compose -f compose.yaml -f compose.demo.yaml ps
curl -s http://localhost:8083/connectors/sales-postgres/status
curl -s http://localhost:8083/connectors/warehouse-postgres/status
```

Endpoints:

- Kafka: `localhost:9092` (`SASL_PLAINTEXT` / `SCRAM-SHA-512`, user `cdc-consumer` / `admin`)
- Schema Registry: http://localhost:8081
- Kafbat UI: http://localhost:8082
- Kafka Connect REST: http://localhost:8083
- demo DB `sales`: `localhost:5433` (`postgres` / `postgres`)
- demo DB `warehouse`: `localhost:5434` (`postgres` / `postgres`)
- Node consumer DB: `localhost:5435` (`postgres` / `postgres`)

Snapshot rows from `sales` (`customers`, `orders`, `order_items`) and
`warehouse` (`products`, `warehouses`, `stock_levels`) are already published.
Trigger a live change:

```bash
docker compose -f compose.yaml -f compose.demo.yaml exec postgres-sales \
  psql -U postgres -d sales -c \
  "UPDATE customers SET full_name = 'Ada Byron' WHERE email = 'ada@example.test';"
```

Inspect `sales.public.customers` in Kafbat, or start either consumer:

- [`examples/node`](examples/node) — durable service-local processing with pg-boss
- [`examples/python`](examples/python) — direct Kafka decoding PoC

Stop without deleting Kafka or PostgreSQL data:

```bash
docker compose -f compose.yaml -f compose.demo.yaml down
```

Add `--volumes` only for a clean slate.

## UIs and tools

| Service | Address | Browser UI | Login |
| --- | --- | --- | --- |
| Kafka | `localhost:9092` | no | `SCRAM-SHA-512` (`cdc-consumer` / `admin`) |
| Schema Registry | http://localhost:8081 | REST only | none |
| Kafbat UI | http://localhost:8082 | yes | `admin` / `admin` |
| Kafka Connect REST | http://localhost:8083 | no | — |

Kafbat is the Kafka browser for per-table topics, messages, connectors, and
registry schemas. Schema Registry itself is the Confluent REST API at
http://localhost:8081 (`/subjects`, `/schemas/ids/{id}`).

## Kafka authentication

Client listeners use `SASL_PLAINTEXT` and `SCRAM-SHA-512`. There is no TLS in
this demo; production should use `SASL_SSL`.

| Principal | Used by | Kafka ACL sketch |
| --- | --- | --- |
| `admin` | superuser | none (authorizer bypass) |
| `connect` | Debezium / Kafka Connect | all on `sales.`, `warehouse.`, `cdc.`; group `cdc-connect`; cluster create/describe/alter |
| `schema-registry` | Schema Registry | all on `_schemas`; group `schema-registry` |
| `kafbat` | Kafbat UI Kafka client | read/describe topics and groups |
| `cdc-consumer` | Node and Python examples | read `sales.` / `warehouse.`; groups `cdc-consumer*` and `example-*` |

`KAFBAT_USER` is only the Kafbat **web** login. The Kafka principal is
`KAFKA_KAFBAT_USER` (`kafbat`).

The broker also has a docker-only `INTERNAL` PLAINTEXT listener on
`kafka:29092`. Kafka requires that inter-broker listener in
`advertised.listeners`, so it is advertised inside Compose but **not**
published to the host. `User:ANONYMOUS` is a superuser there so
`kafka-auth-init` can create SCRAM users and ACLs without baking credentials
into `kafka-storage format`. Application clients must use `kafka:29094` or
`localhost:9092`. Anyone on the Compose network can still reach `29092`
without a password; treat that network as trusted.

If you change listeners on an existing volume and Kafka will not start, remove
only the Kafka volume (`cdc-stack-kafka-data`) and bring the stack up again.

## Deploy the shared infrastructure

On a real host use only [`compose.yaml`](compose.yaml):

```bash
cp .env.example .env
docker compose up --build -d
```

Set the two PostgreSQL hostnames, credentials, publications, and slots first.
Each source database needs logical replication:

```text
wal_level=logical
max_replication_slots>=1
max_wal_senders>=1
```

[`postgres/sales/init/03_debezium.sql`](postgres/sales/init/03_debezium.sql)
and [`postgres/warehouse/init/03_debezium.sql`](postgres/warehouse/init/03_debezium.sql)
show the replication role, grants, and publication.

After changing `docker/kafka-connect/postgres.config.json`, recreate connector
registration jobs:

```bash
docker compose -f compose.yaml -f compose.demo.yaml up -d --build --force-recreate \
  kafka-connect-init-sales kafka-connect-init-warehouse
```

## Replica identity

Tables published for `UPDATE` and `DELETE` need a stable row identity.
Debezium uses the same identity as the Kafka record key.

The default is the primary key. For a table without one:

1. add a primary key (preferred);
2. use `REPLICA IDENTITY USING INDEX` with a unique, non-partial,
   non-deferrable index whose columns are `NOT NULL`;
3. use `REPLICA IDENTITY FULL` only as a heavier last resort.

For example:

```sql
ALTER TABLE public.course_waitlist
  ADD PRIMARY KEY (course_id, user_id);
```

Check the setting:

```sql
SELECT n.nspname AS schema, c.relname AS table, c.relreplident
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relname = 'course_waitlist';
-- d = DEFAULT, i = index, f = FULL, n = NOTHING
```

## State and persistence

| State | Where it lives | Volume |
| --- | --- | --- |
| Kafka CDC topics | Kafka log dirs | `kafka-data` |
| Connect configs, offsets, status | `cdc.connect-*` topics | `kafka-data` |
| Debezium schema history | topic per connector | `kafka-data` |
| Schema Registry subjects | `_schemas` topic | `kafka-data` |
| Demo source data | PostgreSQL | `postgres-sales-data`, `postgres-warehouse-data` |
| Node jobs and DLQ | service-owned PostgreSQL | `postgres-consumer-data` |

Schema Registry stores Avro subjects in the compacted `_schemas` topic.

## Event model

- Every captured table gets its own compacted topic, for example
  `sales.public.orders`.
- Topics have one partition in this demo. Ordering is per table; Kafka keys
  preserve ordering for one row.
- Values use Confluent-wire Avro (`0x00`, schema ID, payload).
- Subjects follow TopicNameStrategy, so each table is
  `{topic}-key` / `{topic}-value` — for example
  `sales.public.orders-value` and `warehouse.public.products-key`.
- The Kafka record key is the definitive row identity, including composite
  primary keys.
- Debezium emits a normal `op=d` envelope and then a null tombstone. Consumers
  process the delete envelope and skip the tombstone.

## Consumer ownership and replay

Every application uses its own Kafka consumer group. The shared stack neither
knows nor operates application queues.

The Node example demonstrates the boundary:

1. Decode the generic Avro envelope and key through Schema Registry. No table-specific
   Zod schema runs on the Kafka path.
2. Insert a JSON-safe job into service-owned pg-boss using a deterministic ID
   derived from topic/partition/offset (plus a run ID for explicit replay).
3. Commit the Kafka offset only after the PostgreSQL insert succeeds.
4. Let pg-boss run table-specific Zod and business handlers. Handler failures
   retry with exponential backoff and end in a local DLQ; Kafka keeps moving.
5. Apply last-write-wins per `(table, primary key)` using Debezium `source.lsn`.
   Move the watermark only after the projection write succeeds.

The deterministic live job ID closes the Kafka/PostgreSQL dual-write gap: a
crash after enqueue but before offset commit causes redelivery, but the
duplicate job insert is harmless. Replay gets a per-run namespace so another
intentional replay can execute the same latest coordinate again.

For a full replay, the Node example reads every table topic from offset zero up
to startup high watermarks and enqueues those jobs before starting live
consumption. Replay accepts `LSN == watermark` so the latest event is applied
again; older events remain stale.

Child-before-parent delivery does not require buffering the whole log. A child
job can fail its foreign key and retry while the independent parent job
succeeds. pg-boss uses `key_strict_fifo`, so one failed row blocks successors
for that row but not unrelated rows.

One caveat remains independent of Kafka or the job system: replaying an
intermediate parent delete executes `ON DELETE CASCADE`. Local-only dependent
state does not reappear merely because a later CDC event re-inserts the parent.
