# Kafka, Debezium, and Confluent Schema Registry CDC stack

Shared CDC infrastructure for dev, staging, and production:

```text
PostgreSQL (your databases, outside this compose)
  -> Debezium on Kafka Connect
  -> one compacted Kafka topic per table (`{prefix}.{schema}.{table}`)
  -> application-owned Kafka consumer
```

[`compose.yaml`](compose.yaml) is the environment stack: Kafka, Schema Registry,
Kafka Connect, and Kafbat. It does not start databases or register connectors.
You add each source in Kafbat after the source PostgreSQL is prepared.

## Deploy

You need Docker Compose v2 and free host ports `9092`, `8081`–`8083`.

```bash
cp .env.example .env          # 2 vCPU / 4 GiB
# or: cp .env.prod.example .env   # 2 vCPU / 8 GiB
docker compose up -d kafka --wait
docker compose --profile init run --rm kafka-auth-init
docker compose up --build -d
```

Step-by-step for a real host is at the bottom.

Endpoints:

- Kafka: `localhost:9092` (`SASL_PLAINTEXT` / `SCRAM-SHA-512`)
- Schema Registry: http://localhost:8081
- Kafbat UI: http://localhost:8082 (`KAFBAT_USER` / `KAFBAT_PASSWORD`)
- Kafka Connect REST: http://localhost:8083 (no login; keep it off the public internet)

Connectors are not created at startup. After Kafka Connect is healthy, add them
in Kafbat.

## Add a Debezium connector in Kafbat

Kafbat talks to Kafka Connect, so you can create, edit, restart, and delete
connectors in the UI (Kafka Connect → new connector → `PostgresConnector`).

Kafbat does **not** prepare PostgreSQL. Each source database still needs:

```text
wal_level=logical
max_replication_slots>=1
max_wal_senders>=1
```

Role, grants, and publication:

```sql
CREATE ROLE debezium WITH LOGIN REPLICATION PASSWORD 'change-me';
GRANT CONNECT ON DATABASE sales TO debezium;
GRANT USAGE ON SCHEMA public TO debezium;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO debezium;
CREATE PUBLICATION sales_debezium_pub FOR TABLES IN SCHEMA public;
```

Copy [`docker/kafka-connect/postgres.connector.example.json`](docker/kafka-connect/postgres.connector.example.json)
and change hostname, database, user, password, `topic.prefix`, slot, and
publication. Keep:

- Avro converters pointing at `http://schema-registry:8081`
- schema history on `kafka:29094` with the `connect` SCRAM user
- `publication.autocreate.mode=disabled`

## Kafka authentication

Client listeners use `SASL_PLAINTEXT` and `SCRAM-SHA-512`. There is no TLS in
this compose; production traffic across a network should use `SASL_SSL`.

| Principal | Used by | Kafka ACL |
| --- | --- | --- |
| `admin` | superuser | authorizer bypass |
| `connect` | Debezium / Kafka Connect | all topics and the `cdc-connect` group |
| `schema-registry` | Schema Registry | `_schemas` |
| `kafbat` | Kafbat Kafka client | read/describe topics and groups |
| `*` | every authenticated user | read/describe all topics and groups |

`KAFBAT_USER` is only the Kafbat **web** login. The Kafka principal is
`KAFKA_KAFBAT_USER` (`kafbat`).

`connect` is allowed on all topics so a connector added in Kafbat with a new
`topic.prefix` does not need a compose change. A new SCRAM user only needs
credentials; `User:*` already grants read. Write still requires an extra ACL.

The broker also has a docker-only `INTERNAL` PLAINTEXT listener on
`kafka:29092`. Kafka requires that inter-broker listener in
`advertised.listeners`, so it is advertised inside Compose but **not**
published to the host. `User:ANONYMOUS` is a superuser there so
`kafka-auth-init` seeds SCRAM users (Compose profile `init`; not started by a
plain `up`). Run it once after Kafka is healthy and before Schema Registry /
Connect. Application clients must use `kafka:29094` or `localhost:9092`.
Treat the Compose network as trusted.

If you change listeners on an existing volume and Kafka will not start, remove
only the Kafka volume and bring the stack up again.

## Nowy użytkownik Kafka po postawieniu stacku

Robisz to na już działającym brokerze. Konta i ACL-e są w metadanych Kafki
(volume `kafka-data`), nie w `compose.yaml`. Kont aplikacji nie dopisuj do
`.env`, chyba że to serwis stacku (`connect`, `schema-registry`, `kafbat`).

Komendy administracyjne idą przez nasłuch INTERNAL (tylko w sieci Dockera).
Z hosta wejdź do kontenera Kafki (port `29092` nie jest wystawiony na zewnątrz):

```bash
docker compose exec kafka bash
```

W kontenerze bootstrap to `localhost:29092`. `User:ANONYMOUS` jest tam
superuserem, więc te polecenia nie potrzebują SASL.

Założenie konta albo zmiana hasła (ta sama komenda nadpisuje credential).
`--entity-name user-name` to login, `password=twoje-haslo` to hasło —
podmień oba:

```bash
/opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server localhost:29092 \
  --alter --entity-type users --entity-name user-name \
  --add-config 'SCRAM-SHA-512=[password=twoje-haslo]'
```

Lista użytkowników:

```bash
/opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server localhost:29092 \
  --describe --entity-type users
```

Usunięcie credentiala SCRAM (principal nie zaloguje się):

```bash
/opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server localhost:29092 \
  --alter --entity-type users --entity-name user-name \
  --delete-config SCRAM-SHA-512
```

Klienci nadal łączą się na `localhost:9092` (albo `kafka:29094` w sieci
Compose) przez `SASL_PLAINTEXT` / `SCRAM-SHA-512`.

### Uprawnienia (ACL)

`ALLOW_EVERYONE_IF_NO_ACL_FOUND` jest `false`, więc brak ACL-a to odmowa.
Init już daje `User:*` **Read** i **Describe** na wszystkich topikach i grupach
oraz **Describe** na klastrze. Nowy użytkownik SCRAM może więc czytać topiki
CDC bez osobnego ACL-a na siebie. **Write**, **Create** i idempotentny produce
wymagają jawnego allow.

Lista ACL-i:

```bash
/opt/kafka/bin/kafka-acls.sh \
  --bootstrap-server localhost:29092 \
  --list
```

Zapis na jeden topik:

```bash
/opt/kafka/bin/kafka-acls.sh \
  --bootstrap-server localhost:29092 \
  --add \
  --allow-principal User:user-name \
  --operation Write --operation Describe \
  --topic topic-name
```

Zapis na wszystkie topiki o wspólnym przedrostku (tu: `topic-name.`):

```bash
/opt/kafka/bin/kafka-acls.sh \
  --bootstrap-server localhost:29092 \
  --add \
  --allow-principal User:user-name \
  --operation Write --operation Describe \
  --topic topic-name. --resource-pattern-type prefixed
```

Topiki CDC pisze Connect użytkownikiem `connect` — aplikacji nie dawaj na nie
zapisu. Powyższe jest dla własnych topików aplikacji.

Idempotentny producer (`enable.idempotence=true`) potrzebuje też
`IdempotentWrite` na klastrze:

```bash
/opt/kafka/bin/kafka-acls.sh \
  --bootstrap-server localhost:29092 \
  --add \
  --allow-principal User:user-name \
  --cluster --operation IdempotentWrite
```

Aplikacja tylko konsumująca zwykle **nie potrzebuje dodatkowego ACL-a**:
`User:*` już pokrywa odczyt. Gdybyś kiedyś zabrał wildcard, daj temu
principalowi Read/Describe na topiku i na jego consumer group:

```bash
/opt/kafka/bin/kafka-acls.sh \
  --bootstrap-server localhost:29092 \
  --add \
  --allow-principal User:user-name \
  --operation Read --operation Describe \
  --topic topic-name

/opt/kafka/bin/kafka-acls.sh \
  --bootstrap-server localhost:29092 \
  --add \
  --allow-principal User:user-name \
  --operation Read --operation Describe \
  --group group-name
```

Zdjęcie allow (bez `--resource-pattern-type prefixed`, chyba że tak zakładałeś):

```bash
/opt/kafka/bin/kafka-acls.sh \
  --bootstrap-server localhost:29092 \
  --remove \
  --allow-principal User:user-name \
  --operation Write --operation Describe \
  --topic topic-name
```

Kont aplikacji nie wstawiaj do `KAFKA_SUPER_USERS` — to omija ACL-e.
Jedyny nazwany superuser poza `ANONYMOUS` na INTERNAL ma zostać `admin`.

## Replica identity

Tables published for `UPDATE` and `DELETE` need a stable row identity.
Debezium uses the same identity as the Kafka record key.

The default is the primary key. For a table without one:

1. add a primary key (preferred);
2. use `REPLICA IDENTITY USING INDEX` with a unique, non-partial,
   non-deferrable index whose columns are `NOT NULL`;
3. use `REPLICA IDENTITY FULL` only as a heavier last resort.

```sql
ALTER TABLE public.course_waitlist
  ADD PRIMARY KEY (course_id, user_id);
```

## Event model

- Every captured table gets its own compacted topic, for example
  `sales.public.orders`.
- This compose uses one partition per topic. Ordering is per table; Kafka keys
  preserve ordering for one row.
- Values use Confluent-wire Avro (`0x00`, schema ID, payload).
- Subjects follow TopicNameStrategy: `{topic}-key` / `{topic}-value`.
- The Kafka record key is the definitive row identity, including composite
  primary keys.
- Debezium emits a normal `op=d` envelope and then a null tombstone.

## State

| State | Where it lives | Volume |
| --- | --- | --- |
| Kafka CDC topics | Kafka log dirs | `kafka-data` |
| Connect configs, offsets, status | `cdc.connect-*` topics | `kafka-data` |
| Debezium schema history | topic per connector | `kafka-data` |
| Schema Registry subjects | `_schemas` topic | `kafka-data` |

## Uruchomienie na dev, staging albo production

Raz na każde środowisko, na hoście z Docker Compose v2. Heap z
[`.env.prod.example`](.env.prod.example) jest pod **2 vCPU / 8 GiB RAM**.
[`.env.example`](.env.example) to mniejszy profil na 4 GiB.

1. Skopiuj to repozytorium na host.
2. Skopiuj plik env i go uzupełnij:

   ```bash
   cp .env.prod.example .env
   ```

3. Jeśli na jednym Dockerze może stanąć więcej niż jeden stack, daj
   środowisku własne nazwy, np. `cdc-dev`, `cdc-stg`, `cdc-prod`:

   ```bash
   STACK_NAME=cdc-prod
   NETWORK_NAME=cdc-prod-network
   KAFKA_VOLUME_NAME=cdc-prod-kafka-data
   ```

4. Ustaw `KAFKA_ADVERTISED_HOST` na DNS albo IP, które widzą klienci
   (nie `localhost`, chyba że klienci są na tym samym hoście).
5. Wygeneruj **nowe** `KAFKA_CLUSTER_ID` dla tego środowiska:

   ```bash
   docker run --rm apache/kafka:4.3.1 /opt/kafka/bin/kafka-storage.sh random-uuid
   ```

   Wklej wartość do `.env`. Nie używaj ID z innego środowiska.
6. Domyślne hasła Kafki i Kafbata to `admin`. Zmień je, jeśli host nie jest
   w zamkniętej sieci. `KAFBAT_USER` to tylko login do UI;
   `KAFKA_KAFBAT_USER` to konto Kafka.
7. Otwórz w firewallu tylko to, czego potrzebują klienci: `9092` (Kafka),
   `8081` (Schema Registry), `8082` (Kafbat). `8083` (Connect REST) trzymaj
   z dala od internetu — Kafbat woła go wewnątrz sieci Compose.
8. Uruchom stack (init SCRAM/ACL zanim wstaną Schema Registry i Connect):

   ```bash
   docker compose --env-file .env up -d kafka --wait
   docker compose --env-file .env --profile init run --rm kafka-auth-init
   docker compose --env-file .env up --build -d
   ```

   Przy kolejnych startach na tym samym volume Kafki init nie jest
   obowiązkowy — użytkownicy i ACL-e już są w metadanych. Po `down -v`
   albo nowym `KAFKA_CLUSTER_ID` odpal go znowu.

9. Poczekaj, aż Kafka, Schema Registry, Kafka Connect i Kafbat będą healthy:

   ```bash
   docker compose ps
   ```

10. Na każdym źródłowym PostgreSQL włącz logical replication, załóż rolę
    `debezium`, granty i publication (patrz wyżej).
11. W Kafbat wejdź w Kafka Connect i utwórz `PostgresConnector` ze wzoru
    [`docker/kafka-connect/postgres.connector.example.json`](docker/kafka-connect/postgres.connector.example.json).
    Wskaż tę bazę. W polach JAAS schema history wstaw hasło SCRAM użytkownika
    `connect` z `.env`.
12. Sprawdź, że connector jest `RUNNING` oraz że widać topiki
    `{prefix}.{schema}.{table}` i subjecty w Schema Registry.

Zatrzymanie bez kasowania danych Kafki: `docker compose down`.  
`--volumes` tylko gdy chcesz pusty klaster (przy następnym starcie daj też
nowe `KAFKA_CLUSTER_ID`).
