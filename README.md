# Kafka, Debezium, Apicurio, and RabbitMQ CDC stack

Standalone Docker Compose stack that streams PostgreSQL changes through:

```text
PostgreSQL (external, one database per connector)
  -> Debezium on Kafka Connect
  -> one compacted Kafka topic per source
  -> Redpanda Connect byte-for-byte bridge
  -> RabbitMQ topic exchange
```

Apicurio stores Avro schemas, and Kafbat UI provides access to Kafka, Kafka
Connect, and the registry.

[`compose.yaml`](compose.yaml) is infrastructure only: Kafka, Connect,
Apicurio, RabbitMQ, the bridge, and connector registration. It does **not**
run PostgreSQL. Point the connectors at your own databases with environment
variables.

Sample sales and warehouse databases live in
[`compose.demo.yaml`](compose.demo.yaml) for local exploration.

## Repository layout

```text
.
├── compose.yaml
├── compose.demo.yaml
├── .env.example
├── .env.demo.example
├── config/rabbitmq-bridge/
├── docker/apicurio/
├── docker/kafka-connect/
├── postgres/sales/
├── postgres/warehouse/
└── examples/
    ├── node/
    └── python/
```

- [`examples/node`](examples/node) — NestJS consumer: optional full Kafka
  replay at startup, then live delivery from RabbitMQ.
- [`examples/python`](examples/python) — Python consumer reading the compacted
  Kafka topics directly.

## Try it locally

You need Docker Compose v2 and free host ports `9092`, `8081`–`8083`,
`5672`, `15672`, `5433`, and `5434`. First start builds the Connect and
Apicurio images and can take a few minutes.

```bash
cp .env.demo.example .env
docker compose -f compose.yaml -f compose.demo.yaml up --build -d
```

Wait until both Debezium connectors are running (retry until `RUNNING`):

```bash
docker compose -f compose.yaml -f compose.demo.yaml ps
curl -s http://localhost:8083/connectors/sales-postgres/status
curl -s http://localhost:8083/connectors/warehouse-postgres/status
```

Endpoints:

- Kafka: `localhost:9092`
- Apicurio: http://localhost:8081
- Kafbat UI: http://localhost:8082
- Kafka Connect REST: http://localhost:8083
- RabbitMQ AMQP: `localhost:5672`
- RabbitMQ management: http://localhost:15672
- Demo DB `sales`: `localhost:5433` (`postgres` / `postgres`)
- Demo DB `warehouse`: `localhost:5434` (`postgres` / `postgres`)

See [UIs and tools](#uis-and-tools) for logins and which of these are browsers.

Snapshot rows from `sales` (`customers`, `orders`, `order_items`) and
`warehouse` (`products`, `warehouses`, `stock_levels`) are already in the
compacted topics. Trigger a live change:

```bash
docker compose -f compose.yaml -f compose.demo.yaml exec postgres-sales \
  psql -U postgres -d sales -c \
  "UPDATE customers SET full_name = 'Ada Byron' WHERE email = 'ada@example.test';"
```

Watch the bridge, then inspect the event in Kafbat (`sales.cdc`) or RabbitMQ
(exchange `cdc.events`, routing key `sales.public.customers`):

```bash
docker compose -f compose.yaml -f compose.demo.yaml logs -f cdc-rabbitmq-bridge
```

Optional consumers (stack must already be up):

- [`examples/python`](examples/python) — read both Kafka topics
- [`examples/node`](examples/node) — bind RabbitMQ queues and optionally replay Kafka

Stop without deleting Kafka, RabbitMQ, or demo Postgres data:

```bash
docker compose -f compose.yaml -f compose.demo.yaml down
```

Add `--volumes` only if you want a clean slate.

After changing `docker/kafka-connect/postgres.config.json`, recreate the
registration jobs with the same `-f` flags:

```bash
docker compose -f compose.yaml -f compose.demo.yaml up -d --build --force-recreate \
  kafka-connect-init-sales kafka-connect-init-warehouse
```

## UIs and tools

All of these bind on the host when the stack is up. Demo logins match
`.env.demo.example`; on a real deploy they come from `.env`.

| Service | Address | UI tool | Login |
| --- | --- | --- | --- |
| Kafka | `localhost:9092` | no | — |
| Apicurio Registry | http://localhost:8081 (UI: http://localhost:8081/ui) | yes | none |
| Kafbat UI | http://localhost:8082 | yes | `admin` / `admin` |
| Kafka Connect REST | http://localhost:8083 | no | — |
| RabbitMQ AMQP | `localhost:5672` | no | `admin` / `admin` |
| RabbitMQ management | http://localhost:15672 | yes | `admin` / `admin` |

Kafbat is the Kafka browser: topics (`sales.cdc`, `warehouse.cdc`), messages,
Connectors, and the schema registry. Apicurio is the Avro registry (also
http://localhost:8081/apis/ccompat/v7). RabbitMQ management shows the
`cdc.events` topic exchange; queues appear only after an application binds
them.

## Deploy the infrastructure

On a real host, use only [`compose.yaml`](compose.yaml). Copy
[`.env.example`](.env.example) to `.env`, set the two PostgreSQL hostnames,
credentials, publications, and slots, then:

```bash
cp .env.example .env
docker compose up --build -d
```

Each source database must have logical replication enabled:

```text
wal_level=logical
max_replication_slots>=1
max_wal_senders>=1
```

Create a replication role, `SELECT` grants, and a publication that match
`CDC_*_SLOT_NAME` / `CDC_*_PUBLICATION_NAME` before the connectors start.
[`postgres/sales/init/03_debezium.sql`](postgres/sales/init/03_debezium.sql)
and [`postgres/warehouse/init/03_debezium.sql`](postgres/warehouse/init/03_debezium.sql)
show the pattern.

### Replica identity

Tables published for `UPDATE` and `DELETE` need a way for PostgreSQL logical
replication to identify the changed row. Debezium also uses that identity as
the Kafka record key (together with `__source_table` in this stack).

By default, replica identity is `DEFAULT`: PostgreSQL uses the primary key.
If a table has no primary key, an `UPDATE` or `DELETE` on it fails with an
error such as *cannot update table "…" because it does not have a replica
identity and publishes updates*. `INSERT` still works.

Typical fixes, from most common to least:

1. Add a primary key (preferred). Compaction then has a stable key per row.
2. `ALTER TABLE … REPLICA IDENTITY USING INDEX …` on a unique, non-partial,
   non-deferrable index whose columns are `NOT NULL`.
3. `ALTER TABLE … REPLICA IDENTITY FULL` — the whole old row is the identity.
   Use this only when there is no natural unique key. It is heavier in WAL
   and in Debezium payloads.

Example with a made-up table that has no primary key:

```sql
CREATE TABLE public.course_waitlist (
  course_id  uuid NOT NULL,
  user_id    uuid NOT NULL,
  joined_at  timestamptz NOT NULL DEFAULT now()
);
```

Inserts replicate. Updating `joined_at` or deleting a row does not, until you
pick an identity. Preferred:

```sql
ALTER TABLE public.course_waitlist
  ADD PRIMARY KEY (course_id, user_id);
```

If a unique constraint already exists and you cannot add a PK:

```sql
CREATE UNIQUE INDEX course_waitlist_course_user_uidx
  ON public.course_waitlist (course_id, user_id);

ALTER TABLE public.course_waitlist
  REPLICA IDENTITY USING INDEX course_waitlist_course_user_uidx;
```

Last resort, when rows are not uniquely identifiable:

```sql
ALTER TABLE public.course_waitlist REPLICA IDENTITY FULL;
```

Check the current setting:

```sql
SELECT n.nspname AS schema, c.relname AS table, c.relreplident
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relname = 'course_waitlist';
-- relreplident: d = DEFAULT (PK), i = index, f = FULL, n = NOTHING
```

## State and persistence

Everything survives `docker compose down` and a restart; only
`down --volumes` wipes it.

| State | Where it lives | Volume |
| --- | --- | --- |
| Kafka topics (CDC data) | Kafka log dirs | `kafka-data` |
| Kafka Connect configs, offsets, status | Kafka topics `cdc.connect-*` | `kafka-data` |
| Debezium schema history | Kafka topic per connector | `kafka-data` |
| Apicurio schemas | Kafka topic `kafkasql-journal` | `kafka-data` |
| RabbitMQ queues, bindings, messages | RabbitMQ mnesia | `rabbitmq-data` |
| Demo PostgreSQL data | Postgres data dir | `postgres-*-data` (demo only) |

Apicurio runs the KafkaSQL storage variant, so it has no disk of its own: it
rebuilds the registry by replaying `kafkasql-journal` on every start.

## Event model

- Each captured database is rerouted to its own compacted topic
  (`sales.cdc` and `warehouse.cdc` by default).
- Each topic has one partition and `cleanup.policy=compact`.
- `__source_table` is `{database}.{schema}.{table}` so primary keys from
  different tables (and sources) do not collide during compaction.
- That value is copied to the `cdc_table` Kafka header. The RabbitMQ bridge
  uses it as the **routing key** (not the Kafka topic name).
- Values are Avro in Confluent wire format (`0x00` plus schema ID and payload).
- Schemas are registered per source table, not per topic: the topic merge uses
  `RegexRouter` (topic only) instead of the reroute transform's schema rename,
  so Apicurio shows `sales.public.orders.Envelope`,
  `warehouse.public.products.Key`, and so on for every captured table.
- Delete tombstones stay in Kafka for compaction. The RabbitMQ bridge skips
  tombstones because the preceding Debezium `op=d` event contains the delete.
- The bridge also copies `kafka_topic` into AMQP headers (`kafka-topic` in
  RabbitMQ) so consumers can look up the correct Avro subject.

## RabbitMQ routing keys (how to bind queues)

The stack declares one durable **topic** exchange (`cdc.events` by default)
and **no queues**. The two compacted Kafka topics are only a transport; they
are **not** RabbitMQ routing keys. Developers declare their own queues and
bind them to `cdc.events`.

Routing key is always `{database}.{schema}.{table}` (Debezium `topic.prefix`
+ Postgres schema + table — not the Kafka topic):

| Change in | Kafka topic | RabbitMQ routing key |
| --- | --- | --- |
| `sales.public.customers` | `sales.cdc` | `sales.public.customers` |
| `sales.public.orders` | `sales.cdc` | `sales.public.orders` |
| `sales.public.order_items` | `sales.cdc` | `sales.public.order_items` |
| `warehouse.public.products` | `warehouse.cdc` | `warehouse.public.products` |
| `warehouse.public.warehouses` | `warehouse.cdc` | `warehouse.public.warehouses` |
| `warehouse.public.stock_levels` | `warehouse.cdc` | `warehouse.public.stock_levels` |

`database` is Debezium `topic.prefix` (usually the Postgres database name).
`schema` is the Postgres schema (`public` in the demo). The same table name
in two schemas gets two routing keys.

Because the exchange type is `topic`, `*` is one dot-separated word and `#`
is everything after. Three-part keys need `#` (or `*.*.*` / `db.schema.*`),
not `sales.*`:

```text
# One table only (typical projection)
queue.declare  my-app.sales.orders
queue.bind     my-app.sales.orders  ->  cdc.events  routing key "sales.public.orders"

# Every table in sales.public
queue.declare  my-app.sales.public
queue.bind     my-app.sales.public   ->  cdc.events  routing key "sales.public.*"

# Every table from the sales database (any schema)
queue.declare  my-app.sales.all
queue.bind     my-app.sales.all      ->  cdc.events  routing key "sales.#"

# Every captured table from both Kafka topics
queue.declare  my-app.cdc.all
queue.bind     my-app.cdc.all       ->  cdc.events  routing key "#"
```

Give each application (or each independent projection) its **own durable
queue name**. Two services binding the same queue compete for messages
(competing consumers). Two services with different queue names each get a
copy (pub/sub).

AMQP headers on every message (hyphenated in RabbitMQ management):

| Header | Meaning |
| --- | --- |
| `cdc-table` | Same as the routing key (`sales.public.orders`) |
| `kafka-topic` | Compacted topic the event came from (`sales.cdc` / `warehouse.cdc`) |
| `kafka-partition` / `kafka-offset` / `kafka-timestamp-ms` | Kafka coordinates |

The body is the unchanged Confluent-wire Avro envelope. Decode with the
schema id in the payload (Apicurio). `kafka-topic` is only extra context,
not the routing key.

The bridge skips Kafka tombstones (`null` values). Deletes still arrive as
a normal Avro event with `op=d` on the same routing key.
