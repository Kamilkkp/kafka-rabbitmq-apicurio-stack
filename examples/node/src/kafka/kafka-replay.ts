import { randomUUID } from 'node:crypto';

import { KafkaJS } from '@confluentinc/kafka-javascript';
import { Inject, Injectable } from '@nestjs/common';

import { cdcTopicOverride, required } from '../config.js';
import { CdcJobQueue } from '../job/cdc-job.queue.js';
import { KafkaCdcRouter } from './kafka-cdc.router.js';

/** librdkafka: -2 = beginning of the partition. */
const OFFSET_BEGINNING = '-2';

function partitionKey(topic: string, partition: number): string {
  return `${topic}:${partition}`;
}

/**
 * Optional one-shot read from offset zero to the startup high watermarks.
 * Jobs are enqueued before the live consumer starts, so pg-boss preserves
 * same-row order while processing both replay and later live work.
 */
@Injectable()
export class KafkaReplay {
  constructor(
    @Inject(CdcJobQueue) private readonly jobs: CdcJobQueue,
    @Inject(KafkaCdcRouter) private readonly router: KafkaCdcRouter,
  ) {}

  async run(): Promise<void> {
    const brokers = required('CDC_KAFKA_BROKERS');
    const kafka = new KafkaJS.Kafka();

    const admin = kafka.admin({ 'bootstrap.servers': brokers });
    await admin.connect();
    const topics = await existingTopics(
      admin,
      cdcTopicOverride ?? this.router.sources(),
    );
    const replayId = randomUUID();
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
        `replay: enqueueing up to ${[...pending]
          .map(([key, offset]) => `${key}@${offset}`)
          .join(', ')}`,
      );

      await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
          const offset = Number(message.offset);
          const last = pending.get(partitionKey(topic, partition));

          if (message.value !== null) {
            await this.jobs.enqueue(
              { topic, partition, offset: message.offset },
              message.value,
              message.key,
              'replay',
              replayId,
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
    }
    console.log('replay: all startup records are durable in pg-boss');
  }
}

/** Ignore owned topics not created by Debezium yet. */
async function existingTopics(admin: {
  fetchTopicOffsets: (topic: string) => Promise<unknown>;
}, candidates: string[]): Promise<string[]> {
  const found: string[] = [];
  for (const topic of candidates) {
    try {
      await admin.fetchTopicOffsets(topic);
      found.push(topic);
    } catch {
      // Not created yet; Debezium may still be taking its initial snapshot.
    }
  }
  return found.sort();
}

function splitPartitionKey(key: string): [string, number] {
  const separator = key.lastIndexOf(':');
  return [key.slice(0, separator), Number(key.slice(separator + 1))];
}
