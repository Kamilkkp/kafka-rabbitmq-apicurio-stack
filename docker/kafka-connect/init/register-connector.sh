#!/bin/sh
set -eu

: "${DATABASE_HOSTNAME:?}"
: "${DATABASE_PORT:?}"
: "${DATABASE_DBNAME:?}"
: "${DEBEZIUM_DATABASE_USER:?}"
: "${DEBEZIUM_DATABASE_PASSWORD:?}"
: "${DEBEZIUM_SOURCE_TOPIC_PREFIX:?}"
: "${DEBEZIUM_SOURCE_SLOT_NAME:?}"
: "${DEBEZIUM_SOURCE_PUBLICATION_NAME:?}"
: "${DEBEZIUM_SOURCE_SCHEMA_INCLUDE_LIST:?}"
: "${DEBEZIUM_APICURIO_URL:?}"
: "${CDC_TOPIC:?}"

CONNECT_URL="${CONNECT_URL:-http://kafka-connect:8083}"
CONNECTOR_NAME="${CONNECTOR_NAME:-postgres-cdc}"
TABLE_INCLUDE_LIST="${DEBEZIUM_SOURCE_TABLE_INCLUDE_LIST:-}"
TABLE_EXCLUDE_LIST="${DEBEZIUM_SOURCE_TABLE_EXCLUDE_LIST:-}"
COLUMN_EXCLUDE_LIST="${DEBEZIUM_SOURCE_COLUMN_EXCLUDE_LIST:-}"
SCHEMA_HISTORY_TOPIC="${DEBEZIUM_SCHEMA_HISTORY_TOPIC:-$DEBEZIUM_SOURCE_TOPIC_PREFIX.schema-history}"
BASE_CONFIG=/etc/kafka-connect/postgres.config.json
RENDERED_CONFIG=/tmp/postgres.config.json

if [ -n "$TABLE_INCLUDE_LIST" ] && [ -n "$TABLE_EXCLUDE_LIST" ]; then
  echo "Set either DEBEZIUM_SOURCE_TABLE_INCLUDE_LIST or DEBEZIUM_SOURCE_TABLE_EXCLUDE_LIST, not both." >&2
  exit 1
fi

echo "Waiting for Kafka Connect..."
attempt=0
until curl --silent --fail "$CONNECT_URL/connector-plugins" >/dev/null; do
  attempt=$((attempt + 1))
  if [ "$attempt" -ge 120 ]; then
    echo "Kafka Connect did not become ready." >&2
    exit 1
  fi
  sleep 1
done

echo "Waiting for Apicurio Registry..."
attempt=0
until curl --silent --fail "$DEBEZIUM_APICURIO_URL/health/ready" >/dev/null; do
  attempt=$((attempt + 1))
  if [ "$attempt" -ge 120 ]; then
    echo "Apicurio Registry did not become ready." >&2
    exit 1
  fi
  sleep 1
done

jq \
  --arg database_hostname "$DATABASE_HOSTNAME" \
  --arg database_port "$DATABASE_PORT" \
  --arg database_dbname "$DATABASE_DBNAME" \
  --arg database_user "$DEBEZIUM_DATABASE_USER" \
  --arg database_password "$DEBEZIUM_DATABASE_PASSWORD" \
  --arg topic_prefix "$DEBEZIUM_SOURCE_TOPIC_PREFIX" \
  --arg slot_name "$DEBEZIUM_SOURCE_SLOT_NAME" \
  --arg publication_name "$DEBEZIUM_SOURCE_PUBLICATION_NAME" \
  --arg schema_include_list "$DEBEZIUM_SOURCE_SCHEMA_INCLUDE_LIST" \
  --arg table_include_list "$TABLE_INCLUDE_LIST" \
  --arg table_exclude_list "$TABLE_EXCLUDE_LIST" \
  --arg column_exclude_list "$COLUMN_EXCLUDE_LIST" \
  --arg schema_history_topic "$SCHEMA_HISTORY_TOPIC" \
  --arg apicurio_url "$DEBEZIUM_APICURIO_URL/apis/registry/v2" \
  --arg collapse_regex "$DEBEZIUM_SOURCE_TOPIC_PREFIX\\..*" \
  --arg collapse_replacement "$CDC_TOPIC" \
  '
    . + {
      "transforms.Collapse.regex": $collapse_regex,
      "transforms.Collapse.replacement": $collapse_replacement,
      "database.hostname": $database_hostname,
      "database.port": $database_port,
      "database.dbname": $database_dbname,
      "database.user": $database_user,
      "database.password": $database_password,
      "topic.prefix": $topic_prefix,
      "slot.name": $slot_name,
      "publication.name": $publication_name,
      "schema.include.list": $schema_include_list,
      "schema.history.internal.kafka.topic": $schema_history_topic,
      "key.converter.apicurio.registry.url": $apicurio_url,
      "value.converter.apicurio.registry.url": $apicurio_url
    }
    | if $table_include_list != "" then .["table.include.list"] = $table_include_list else . end
    | if $table_exclude_list != "" then .["table.exclude.list"] = $table_exclude_list else . end
    | if $column_exclude_list != "" then .["column.exclude.list"] = $column_exclude_list else . end
  ' "$BASE_CONFIG" >"$RENDERED_CONFIG"

echo "Creating or updating connector $CONNECTOR_NAME..."
curl \
  --silent \
  --show-error \
  --fail-with-body \
  --request PUT \
  --header 'Content-Type: application/json' \
  --data-binary "@$RENDERED_CONFIG" \
  "$CONNECT_URL/connectors/$CONNECTOR_NAME/config" >/dev/null

echo "Connector $CONNECTOR_NAME is configured."
