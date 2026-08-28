import { KafkaJS } from '@confluentinc/kafka-javascript';
import { Inject, Injectable } from '@nestjs/common';

import { cdcTopics, required } from '../config.js';
import { KafkaCdcRouter } from './kafka-cdc.router.js';

/** librdkafka: -2 = beginning of the partition. */
const OFFSET_BEGINNING = '-2';

function partitionKey(topic: string, partition: number): string {
  return `${topic}:${partition}`;
}

/**
 * One-shot catch-up: reads every compacted CDC topic from the beginning up to
 * the offsets that existed at startup, feeding the same subscribers RabbitMQ
 * feeds later.
 */
@Injectable()
export class KafkaReplay {
  constructor(
    @Inject(KafkaCdcRouter) private readonly router: KafkaCdcRouter,
  ) {}

  async run(): Promise<void> {
    const brokers = required('CDC_KAFKA_BROKERS');
    const kafka = new KafkaJS.Kafka();

    const admin = kafka.admin({ 'bootstrap.servers': brokers });
    await admin.connect();
    const pending = new Map<string, number>();
    for (const topic of cdcTopics) {
      const watermarks = await admin.fetchTopicOffsets(topic);
      for (const { partition, low, high } of watermarks) {
        if (Number(high) > Number(low)) {
          pending.set(partitionKey(topic, partition), Number(high) - 1);
        }
      }
    }
    await admin.disconnect();

    if (pending.size === 0) {
      console.log(
        `replay: ${cdcTopics.join(', ')} empty, nothing to catch up on`,
      );
      return;
    }

    const consumer = kafka.consumer({
      'bootstrap.servers': brokers,
      'group.id': `${required('CDC_KAFKA_GROUP_ID')}-replay`,
      'enable.auto.commit': false,
      'auto.offset.reset': 'earliest',
    });
    await consumer.connect();
    await consumer.subscribe({ topics: cdcTopics });
    for (const key of pending.keys()) {
      const [topic, partition] = splitPartitionKey(key);
      consumer.seek({ topic, partition, offset: OFFSET_BEGINNING });
    }

    let finish: () => void;
    const drained = new Promise<void>((resolve) => (finish = resolve));

    console.log(
      `replay: up to ${[...pending].map(([key, offset]) => `${key}@${offset}`).join(', ')}`,
    );

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        const offset = Number(message.offset);
        const last = pending.get(partitionKey(topic, partition));

        if (message.value !== null) {
          await this.router.route(
            topic,
            message.value,
            `${topic}[${partition}]@${offset}`,
          );
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
    console.log('replay: finished, switching to RabbitMQ');
  }
}

function splitPartitionKey(key: string): [string, number] {
  const separator = key.lastIndexOf(':');
  return [key.slice(0, separator), Number(key.slice(separator + 1))];
}
