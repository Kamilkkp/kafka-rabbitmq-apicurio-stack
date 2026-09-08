"""
CDC consumer (Python) — Kafka + Confluent Schema Registry via confluent-kafka.

  pip install -r requirements.txt
  set -a && source .env && set +a
  python3 cdc_consumer.py

One-shot smoke (pull ≤N msgs, exit):

  ONESHOT=1 ONESHOT_MAX=5 python3 cdc_consumer.py
"""

from __future__ import annotations

import os
import sys
from typing import Any, Protocol

from confluent_kafka import (
    OFFSET_BEGINNING,
    Consumer,
    KafkaError,
    KafkaException,
    Message,
    TopicPartition,
)
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext
from debezium_logical_types import DebeziumSchemaRegistryClient
from pydantic import BaseModel, Field


def required(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise SystemExit(f"Missing env {name}")
    return value


class ConfluentAvroDecoder:
    """Ready-made Confluent AvroDeserializer against Schema Registry."""

    def __init__(self) -> None:
        config = {"url": required("SCHEMA_REGISTRY_URL").rstrip("/")}
        self._sr = DebeziumSchemaRegistryClient(config)
        self._deserializer = AvroDeserializer(self._sr)

    def decode(self, data: bytes, topic: str) -> dict[str, Any]:
        event = self._deserializer(
            data, SerializationContext(topic, MessageField.VALUE)
        )
        if not isinstance(event, dict):
            raise TypeError(f"expected Avro record dict, got {type(event)}")
        return event


def source_of(event: dict[str, Any]) -> str:
    source = event.get("source") or {}
    db = source.get("db") or ""
    schema = source.get("schema") or ""
    table = source.get("table") or "?"
    parts = [part for part in (db, schema, table) if part]
    return ".".join(parts) if parts else str(table)


class CustomerRow(BaseModel):
    id: str
    email: str
    full_name: str
    model_config = {"extra": "allow"}


class CustomerEvent(BaseModel):
    op: str
    before: CustomerRow | None = None
    after: CustomerRow | None = None
    source: dict[str, Any] = Field(default_factory=dict)
    model_config = {"extra": "allow"}


class ProductRow(BaseModel):
    id: str
    sku: str
    name: str
    model_config = {"extra": "allow"}


class ProductEvent(BaseModel):
    op: str
    before: ProductRow | None = None
    after: ProductRow | None = None
    source: dict[str, Any] = Field(default_factory=dict)
    model_config = {"extra": "allow"}


class Handler(Protocol):
    id: str

    def accepts(self, event: dict[str, Any]) -> bool: ...

    def try_handle(self, event: dict[str, Any]) -> bool: ...


class CustomersHandler:
    id = "sales-customers-v1"

    def accepts(self, event: dict[str, Any]) -> bool:
        return source_of(event) == "sales.public.customers"

    def try_handle(self, event: dict[str, Any]) -> bool:
        parsed = CustomerEvent.model_validate(event)
        row = parsed.after or parsed.before
        print(
            f"[{self.id}] op={parsed.op} "
            f"email={row.email if row else '?'} "
            f"name={row.full_name if row else '?'}"
        )
        return True


class ProductsHandler:
    id = "warehouse-products-v1"

    def accepts(self, event: dict[str, Any]) -> bool:
        return source_of(event) == "warehouse.public.products"

    def try_handle(self, event: dict[str, Any]) -> bool:
        parsed = ProductEvent.model_validate(event)
        row = parsed.after or parsed.before
        print(
            f"[{self.id}] op={parsed.op} "
            f"sku={row.sku if row else '?'} "
            f"name={row.name if row else '?'}"
        )
        return True


class GenericHandler:
    id = "generic-op-log"

    def accepts(self, event: dict[str, Any]) -> bool:
        return bool(event.get("op"))

    def try_handle(self, event: dict[str, Any]) -> bool:
        print(f"[{self.id}] op={event.get('op')} source={source_of(event)}")
        return True


HANDLERS: list[Handler] = [
    CustomersHandler(),
    ProductsHandler(),
    GenericHandler(),
]


def dispatch(event: dict[str, Any]) -> str:
    for handler in HANDLERS:
        if not handler.accepts(event):
            continue
        try:
            if handler.try_handle(event):
                return handler.id
        except Exception:
            continue
    raise RuntimeError(f"unhandled event source={source_of(event)}")


def handle_message(decoder: ConfluentAvroDecoder, message: Message) -> str:
    data = message.value()
    if data is None:
        return "tombstone"
    return dispatch(decoder.decode(data, message.topic()))


def subscription() -> list[str]:
    """Named topics by default.

    This consumer reads Kafka directly and librdkafka has no exclude list, so
    the topics it wants are listed rather than inferred. Set
    `KAFKA_TOPIC_PATTERN` for a regex subscription instead.
    """
    pattern = os.environ.get("KAFKA_TOPIC_PATTERN")
    if pattern:
        return [pattern]
    topics = os.environ.get("KAFKA_TOPICS") or ",".join(
        (
            "sales.public.customers",
            "sales.public.orders",
            "sales.public.order_items",
            "warehouse.public.products",
            "warehouse.public.warehouses",
            "warehouse.public.stock_levels",
        )
    )
    return [topic.strip() for topic in topics.split(",") if topic.strip()]


def seek_topics() -> set[str]:
    return {
        topic.strip()
        for topic in os.environ.get("SEEK_TO_BEGINNING", "").split(",")
        if topic.strip()
    }


def on_assign(consumer: Consumer, partitions: list[TopicPartition]) -> None:
    wanted = seek_topics()
    for partition in partitions:
        if partition.topic in wanted:
            partition.offset = OFFSET_BEGINNING
    consumer.assign(partitions)


def kafka_auth() -> dict[str, str]:
    protocol = os.environ.get("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT")
    if protocol == "PLAINTEXT":
        return {}
    return {
        "security.protocol": protocol,
        "sasl.mechanisms": os.environ.get(
            "KAFKA_SASL_MECHANISM", "SCRAM-SHA-512"
        ),
        "sasl.username": required("KAFKA_SASL_USERNAME"),
        "sasl.password": required("KAFKA_SASL_PASSWORD"),
    }


def create_consumer() -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": required("KAFKA_BROKERS"),
            "group.id": required("KAFKA_GROUP_ID"),
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            **kafka_auth(),
        }
    )


def poll_message(consumer: Consumer, timeout: float = 1.0) -> Message | None:
    message = consumer.poll(timeout)
    if message is None:
        return None
    if message.error():
        if message.error().code() == KafkaError._PARTITION_EOF:
            return None
        raise KafkaException(message.error())
    return message


def run_streaming(decoder: ConfluentAvroDecoder) -> None:
    topics = subscription()
    consumer = create_consumer()
    consumer.subscribe(topics, on_assign=on_assign)
    seek = seek_topics()
    print(
        f"CDC Python Kafka consumer group={required('KAFKA_GROUP_ID')} "
        f"brokers={required('KAFKA_BROKERS')} topics={','.join(topics)}"
        + (f" seek={','.join(sorted(seek))}" if seek else "")
    )
    try:
        while True:
            message = poll_message(consumer)
            if message is None:
                continue
            location = (
                f"{message.topic()}[{message.partition()}]@{message.offset()}"
            )
            try:
                handler_id = handle_message(decoder, message)
                consumer.commit(message=message, asynchronous=False)
                if handler_id != "tombstone":
                    print(f"processed {location} via {handler_id}")
            except Exception as exc:
                print(f"failed {location}: {exc}", file=sys.stderr)
    finally:
        consumer.close()


def run_oneshot(decoder: ConfluentAvroDecoder) -> int:
    topics = subscription()
    max_messages = int(os.environ.get("ONESHOT_MAX", "5"))
    consumer = create_consumer()
    consumer.subscribe(topics, on_assign=on_assign)
    ok = 0
    received = 0
    empty_polls = 0
    try:
        while received < max_messages and empty_polls < 30:
            message = poll_message(consumer)
            if message is None:
                empty_polls += 1
                continue
            empty_polls = 0
            received += 1
            location = (
                f"{message.topic()}[{message.partition()}]@{message.offset()}"
            )
            try:
                handler_id = handle_message(decoder, message)
                consumer.commit(message=message, asynchronous=False)
                if handler_id != "tombstone":
                    print(f"oneshot ok {location} via {handler_id}")
                ok += 1
            except Exception as exc:
                print(f"oneshot fail {location}: {exc}", file=sys.stderr)
    finally:
        consumer.close()

    print(f"oneshot done ok={ok}/{received}")
    return 0 if ok else 1


def main() -> None:
    decoder = ConfluentAvroDecoder()
    if os.environ.get("ONESHOT") == "1":
        raise SystemExit(run_oneshot(decoder))
    run_streaming(decoder)


if __name__ == "__main__":
    main()
