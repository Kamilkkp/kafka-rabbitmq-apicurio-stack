export function required(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(`Missing env ${name}`);
  }
  return value;
}

/**
 * Same policy as `config/rabbitmq-bridge/bridge.yaml`: take every topic the
 * broker reports, then subtract infrastructure ones. Table topics are always
 * `{prefix}.{schema}.{table}`, which none of these patterns can match.
 */
export const cdcTopicExcludes = (
  process.env.CDC_TOPIC_EXCLUDE?.split(',') ?? [
    '^__consumer_offsets$',
    '^__transaction_state$',
    '^__cluster_metadata$',
    '^kafkasql-journal$', // Apicurio Registry storage
    '^[^.]+\\.connect-(configs|offsets|statuses)$', // Kafka Connect state
    '^[^.]+\\.schema-history$', // Debezium DDL history
  ]
)
  .map((pattern) => pattern.trim())
  .filter(Boolean)
  .map((pattern) => new RegExp(pattern));

/** Explicit topic list; when unset, topics are discovered from the broker. */
export const cdcTopicOverride = process.env.CDC_TOPICS
  ? uniqueTopics(process.env.CDC_TOPICS.split(','))
  : undefined;

export function isCdcTableTopic(topic: string): boolean {
  return !cdcTopicExcludes.some((pattern) => pattern.test(topic));
}

export const rabbitExchange = process.env.CDC_RABBITMQ_EXCHANGE ?? 'cdc.events';
/** Each source binding gets its own queue: `<prefix>.<db.schema.table>`. */
export const rabbitQueuePrefix =
  process.env.CDC_RABBITMQ_QUEUE ?? 'cdc-consumer';

/** Sources implemented by this example and therefore given retry/DLQ queues. */
export const exampleCdcSources = [
  'sales.public.customers',
  'sales.public.orders',
  'sales.public.order_items',
  'warehouse.public.products',
  'warehouse.public.warehouses',
  'warehouse.public.stock_levels',
] as const;

/**
 * Replay every compacted Kafka table topic once at startup, then hand over
 * to RabbitMQ. Off by default: normal runs consume live changes over RabbitMQ.
 */
export const kafkaReplayOnStart = process.env.CDC_KAFKA_REPLAY === 'true';

/**
 * Avro subject is `<kafka-topic>-value`. With one topic per table the routing
 * key and the topic name are the same (`sales.public.orders`).
 */
export function topicForRoutingKey(routingKey: string): string {
  return routingKey;
}

function uniqueTopics(topics: string[]): string[] {
  return [...new Set(topics.map((topic) => topic.trim()).filter(Boolean))];
}
