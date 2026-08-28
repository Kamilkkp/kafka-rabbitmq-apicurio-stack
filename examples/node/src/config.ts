export function required(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(`Missing env ${name}`);
  }
  return value;
}

export const salesTopic = process.env.CDC_SALES_TOPIC ?? 'sales.cdc';
export const warehouseTopic = process.env.CDC_WAREHOUSE_TOPIC ?? 'warehouse.cdc';

/** Compacted CDC topics; each is also the Avro subject lookup key. */
export const cdcTopics = uniqueTopics(
  process.env.CDC_TOPICS?.split(',') ?? [salesTopic, warehouseTopic],
);

export const rabbitExchange = process.env.CDC_RABBITMQ_EXCHANGE ?? 'cdc.events';
/** Each source binding gets its own queue: `<prefix>.<db.schema.table>`. */
export const rabbitQueuePrefix =
  process.env.CDC_RABBITMQ_QUEUE ?? 'cdc-consumer';

/**
 * Replay every compacted Kafka topic once at startup, then hand over to
 * RabbitMQ. Off by default: normal runs consume live changes over RabbitMQ only.
 */
export const kafkaReplayOnStart = process.env.CDC_KAFKA_REPLAY === 'true';

export function topicForRoutingKey(routingKey: string): string {
  if (routingKey.startsWith('warehouse.')) {
    return warehouseTopic;
  }
  return salesTopic;
}

function uniqueTopics(topics: string[]): string[] {
  return [...new Set(topics.map((topic) => topic.trim()).filter(Boolean))];
}
