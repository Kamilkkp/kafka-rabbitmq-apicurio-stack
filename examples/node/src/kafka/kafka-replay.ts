import { randomUUID } from 'node:crypto';

import { KafkaJS } from '@confluentinc/kafka-javascript';
import { AmqpConnection } from '@golevelup/nestjs-rabbitmq';
import { Inject, Injectable } from '@nestjs/common';

import { ReplayCoordinator } from '../apply/replay-coordinator.js';
import {
  cdcTopicOverride,
  isCdcTableTopic,
  rabbitExchange,
  required,
} from '../config.js';

/** librdkafka: -2 = beginning of the partition. */
const OFFSET_BEGINNING = '-2';

function partitionKey(topic: string, partition: number): string {
  return `${topic}:${partition}`;
}

/**
 * One-shot publisher from compacted per-table Kafka topics to the same
 * RabbitMQ exchange used for live traffic. Application handlers therefore
 * have exactly one ingestion path.
 */
@Injectable()
export class KafkaReplay {
  constructor(
    @Inject(AmqpConnection) private readonly amqp: AmqpConnection,
    @Inject(ReplayCoordinator)
    private readonly coordinator: ReplayCoordinator,
  ) {}

  async run(): Promise<void> {
    const brokers = required('CDC_KAFKA_BROKERS');
    const kafka = new KafkaJS.Kafka();

    const admin = kafka.admin({ 'bootstrap.servers': brokers });
    await admin.connect();
    const topics = await discoverTopics(admin);
    const pending = new Map<string, number>();
    for (const topic of topics) {
      const watermarks = await admin.fetchTopicOffsets(topic);
      for (const { partition, low, high } of watermarks) {
        if (Number(high) > Number(low)) {
          pending.set(partitionKey(topic, partition), Number(high) - 1);
        }
      }
    }
    await admin.disconnect();

    const replayId = randomUUID();
    this.coordinator.begin(replayId, topics);

    if (pending.size > 0) {
      const consumer = kafka.consumer({
        'bootstrap.servers': brokers,
        'group.id': `${required('CDC_KAFKA_GROUP_ID')}-replay`,
        'enable.auto.commit': false,
        'auto.offset.reset': 'earliest',
      });
      await consumer.connect();
      await consumer.subscribe({ topics });
      for (const key of pending.keys()) {
        const [topic, partition] = splitPartitionKey(key);
        consumer.seek({ topic, partition, offset: OFFSET_BEGINNING });
      }

      let finish: () => void;
      const drained = new Promise<void>((resolve) => (finish = resolve));

      console.log(
        `replay ${replayId}: publishing up to ${[...pending]
          .map(([key, offset]) => `${key}@${offset}`)
          .join(', ')}`,
      );

      await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
          const offset = Number(message.offset);
          const last = pending.get(partitionKey(topic, partition));

          if (message.value !== null) {
            await this.amqp.publish(rabbitExchange, topic, message.value, {
              persistent: true,
              contentType: 'application/avro',
              messageId: `${topic}[${partition}]@${offset}`,
              headers: {
                'cdc-mode': 'replay',
                'cdc-replay-id': replayId,
                // Same row identity the bridge forwards; see bridge.yaml.
                // amqplib omits a header whose value is undefined.
                'kafka-key': message.key?.toString('base64'),
                'kafka-topic': topic,
                'kafka-partition': partition,
                'kafka-offset': message.offset,
              },
            });
          }

          if (last !== undefined && offset >= last) {
            pending.delete(partitionKey(topic, partition));
            if (pending.size === 0) {
              finish();
            }
          }
        },
      });

      await drained;
      await consumer.disconnect();
    }

    // One marker per routing key reaches the same durable queue after all
    // replay data published for that table.
    for (const topic of topics) {
      await this.amqp.publish(rabbitExchange, topic, Buffer.alloc(0), {
        persistent: true,
        contentType: 'application/octet-stream',
        headers: {
          'cdc-mode': 'replay',
          'cdc-replay-id': replayId,
          'cdc-replay-end': true,
          'kafka-topic': topic,
        },
      });
    }

    console.log(
      `replay ${replayId}: published to RabbitMQ; waiting for queue markers`,
    );
  }
}

/**
 * Mirrors the bridge: list everything the broker has and subtract
 * infrastructure topics, so a newly captured table needs no configuration.
 */
async function discoverTopics(admin: {
  listTopics: () => Promise<string[]>;
  fetchTopicOffsets: (topic: string) => Promise<unknown>;
}): Promise<string[]> {
  if (cdcTopicOverride) {
    const found: string[] = [];
    for (const topic of cdcTopicOverride) {
      try {
        await admin.fetchTopicOffsets(topic);
        found.push(topic);
      } catch {
        // Not created yet; Debezium may still be taking its initial snapshot.
      }
    }
    return found;
  }

  const topics = (await admin.listTopics()).filter(isCdcTableTopic);
  return topics.sort();
}

function splitPartitionKey(key: string): [string, number] {
  const separator = key.lastIndexOf(':');
  return [key.slice(0, separator), Number(key.slice(separator + 1))];
}
