import { KafkaJS } from '@confluentinc/kafka-javascript';
import { Inject, Injectable, type OnApplicationShutdown } from '@nestjs/common';

import { required } from '../config.js';
import { CdcJobQueue } from '../job/cdc-job.queue.js';
import { kafkaAuthConfig } from './kafka-auth.js';
import { KafkaCdcRouter } from './kafka-cdc.router.js';

/**
 * The service's only live ingress. A Kafka offset is committed only after the
 * decoded event is durable in the service-owned PostgreSQL/pg-boss queue.
 */
@Injectable()
export class KafkaConsumer implements OnApplicationShutdown {
  private consumer?: KafkaJS.Consumer;

  constructor(
    @Inject(CdcJobQueue) private readonly jobs: CdcJobQueue,
    @Inject(KafkaCdcRouter) private readonly router: KafkaCdcRouter,
  ) {}

  async start(): Promise<void> {
    const kafka = new KafkaJS.Kafka();
    const consumer = kafka.consumer({
      'bootstrap.servers': required('KAFKA_BROKERS'),
      'group.id': required('KAFKA_GROUP_ID'),
      'enable.auto.commit': false,
      'auto.offset.reset': 'earliest',
      ...kafkaAuthConfig(),
    });
    this.consumer = consumer;

    await consumer.connect();
    await consumer.subscribe({ topics: this.router.sources() });
    await consumer.run({
      partitionsConsumedConcurrently: Number(
        process.env.KAFKA_CONCURRENCY ?? 4,
      ),
      eachMessage: async ({ topic, partition, message }) => {
        if (message.value !== null) {
          await this.jobs.enqueue(
            { topic, partition, offset: message.offset },
            message.value,
            message.key,
            'live',
          );
        }

        await consumer.commitOffsets([
          {
            topic,
            partition,
            offset: (BigInt(message.offset) + 1n).toString(),
          },
        ]);
      },
    });
  }

  async onApplicationShutdown(): Promise<void> {
    await this.consumer?.disconnect();
  }
}
