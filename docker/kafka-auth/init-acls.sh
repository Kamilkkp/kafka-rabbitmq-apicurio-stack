#!/bin/bash
set -euo pipefail

# Seed SCRAM users and ACLs over the unadvertised INTERNAL listener.
# Application traffic never uses this port; clients must authenticate on SASL.

: "${KAFKA_ADMIN_USER:?}"
: "${KAFKA_ADMIN_PASSWORD:?}"
: "${KAFKA_CONNECT_USER:?}"
: "${KAFKA_CONNECT_PASSWORD:?}"
: "${KAFKA_SCHEMA_REGISTRY_USER:?}"
: "${KAFKA_SCHEMA_REGISTRY_PASSWORD:?}"
: "${KAFKA_KAFBAT_USER:?}"
: "${KAFKA_KAFBAT_PASSWORD:?}"
: "${KAFKA_CONSUMER_USER:?}"
: "${KAFKA_CONSUMER_PASSWORD:?}"

BOOTSTRAP="${KAFKA_AUTH_BOOTSTRAP:-kafka:29092}"
CONNECT_GROUP="${CONNECT_GROUP_ID:-cdc-connect}"
SALES_PREFIX="${SALES_TOPIC_PREFIX:-sales}"
WAREHOUSE_PREFIX="${WAREHOUSE_TOPIC_PREFIX:-warehouse}"

upsert_user() {
  local name="$1"
  local password="$2"
  echo "Ensuring SCRAM user $name"
  /opt/kafka/bin/kafka-configs.sh \
    --bootstrap-server "$BOOTSTRAP" \
    --alter \
    --entity-type users \
    --entity-name "$name" \
    --add-config "SCRAM-SHA-512=[password=${password}]"
}

add_acl() {
  echo "ACL: $*"
  local output rc
  set +e
  output="$(/opt/kafka/bin/kafka-acls.sh --bootstrap-server "$BOOTSTRAP" --add "$@" 2>&1)"
  rc=$?
  set -e
  if [ "$rc" -eq 0 ] || printf '%s\n' "$output" | grep -qiE 'already exists|already has'; then
    return 0
  fi
  printf '%s\n' "$output" >&2
  return 1
}

echo "Waiting for Kafka on $BOOTSTRAP..."
attempt=0
until /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server "$BOOTSTRAP" >/dev/null 2>&1; do
  attempt=$((attempt + 1))
  if [ "$attempt" -ge 60 ]; then
    echo "Kafka did not become reachable for ACL bootstrap." >&2
    exit 1
  fi
  sleep 1
done

upsert_user "$KAFKA_ADMIN_USER" "$KAFKA_ADMIN_PASSWORD"
upsert_user "$KAFKA_CONNECT_USER" "$KAFKA_CONNECT_PASSWORD"
upsert_user "$KAFKA_SCHEMA_REGISTRY_USER" "$KAFKA_SCHEMA_REGISTRY_PASSWORD"
upsert_user "$KAFKA_KAFBAT_USER" "$KAFKA_KAFBAT_PASSWORD"
upsert_user "$KAFKA_CONSUMER_USER" "$KAFKA_CONSUMER_PASSWORD"

# Connect produces CDC and owns its worker topics / group.
add_acl --allow-principal "User:${KAFKA_CONNECT_USER}" --operation All --topic "$SALES_PREFIX" --resource-pattern-type prefixed
add_acl --allow-principal "User:${KAFKA_CONNECT_USER}" --operation All --topic "$WAREHOUSE_PREFIX" --resource-pattern-type prefixed
add_acl --allow-principal "User:${KAFKA_CONNECT_USER}" --operation All --topic cdc. --resource-pattern-type prefixed
add_acl --allow-principal "User:${KAFKA_CONNECT_USER}" --operation All --group "$CONNECT_GROUP" --resource-pattern-type prefixed
add_acl --allow-principal "User:${KAFKA_CONNECT_USER}" --cluster --operation Create --operation Describe --operation DescribeConfigs --operation Alter --operation AlterConfigs --operation IdempotentWrite

# Schema Registry owns the compacted schema log.
add_acl --allow-principal "User:${KAFKA_SCHEMA_REGISTRY_USER}" --operation All --topic '_schemas'
add_acl --allow-principal "User:${KAFKA_SCHEMA_REGISTRY_USER}" --operation All --group schema-registry --resource-pattern-type prefixed
add_acl --allow-principal "User:${KAFKA_SCHEMA_REGISTRY_USER}" --cluster --operation Create --operation Describe --operation DescribeConfigs --operation IdempotentWrite

# Kafbat is read-only on the log.
add_acl --allow-principal "User:${KAFKA_KAFBAT_USER}" --operation Read --operation Describe --operation DescribeConfigs --topic '*' --resource-pattern-type literal
add_acl --allow-principal "User:${KAFKA_KAFBAT_USER}" --operation Describe --operation DescribeConfigs --group '*' --resource-pattern-type literal
add_acl --allow-principal "User:${KAFKA_KAFBAT_USER}" --cluster --operation Describe --operation DescribeConfigs --operation Alter

# Application consumers read CDC and manage their own groups.
add_acl --allow-principal "User:${KAFKA_CONSUMER_USER}" --operation Read --operation Describe --topic "$SALES_PREFIX" --resource-pattern-type prefixed
add_acl --allow-principal "User:${KAFKA_CONSUMER_USER}" --operation Read --operation Describe --topic "$WAREHOUSE_PREFIX" --resource-pattern-type prefixed
add_acl --allow-principal "User:${KAFKA_CONSUMER_USER}" --operation Read --operation Describe --group cdc-consumer --resource-pattern-type prefixed
add_acl --allow-principal "User:${KAFKA_CONSUMER_USER}" --operation Read --operation Describe --group example- --resource-pattern-type prefixed
add_acl --allow-principal "User:${KAFKA_CONSUMER_USER}" --cluster --operation Describe

echo "SCRAM users and ACLs are in place."
