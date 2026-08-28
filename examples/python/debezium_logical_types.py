"""Make fastavro decode Debezium temporal columns as `datetime`.

Debezium marks time columns as a plain `long` with `connect.name`, not with an
Avro `logicalType`, so fastavro returns ints. Stamping the logical type has to
happen before parsing (fastavro drops `connect.name`) and has to cover the
referenced `Value` schema, which is where the table columns live.
"""

from __future__ import annotations

import json
from typing import Any

from attrs import evolve
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient

DEBEZIUM_TIME_TO_LOGICAL = {
    "io.debezium.time.Timestamp": "timestamp-millis",
    "io.debezium.time.MicroTimestamp": "timestamp-micros",
}


def _stamp(node: Any) -> Any:
    if isinstance(node, list):
        return [_stamp(item) for item in node]
    if not isinstance(node, dict):
        return node
    stamped = {key: _stamp(value) for key, value in node.items()}
    logical = DEBEZIUM_TIME_TO_LOGICAL.get(stamped.get("connect.name"))
    if logical is not None and stamped.get("type") == "long":
        stamped["logicalType"] = logical
    return stamped


def _stamped(schema: Schema) -> Schema:
    return evolve(schema, schema_str=json.dumps(_stamp(json.loads(schema.schema_str))))


class DebeziumSchemaRegistryClient(SchemaRegistryClient):
    def get_schema(self, schema_id, subject_name=None, fmt=None, reference_format=None):
        return _stamped(super().get_schema(schema_id, subject_name, fmt, reference_format))

    def get_version(self, subject_name, version="latest", deleted=False, fmt=None):
        registered = super().get_version(subject_name, version, deleted, fmt)
        return evolve(registered, schema=_stamped(registered.schema))
