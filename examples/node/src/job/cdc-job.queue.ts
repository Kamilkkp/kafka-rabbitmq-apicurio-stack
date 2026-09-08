import { Inject, Injectable, type OnApplicationShutdown } from '@nestjs/common';
import { PgBoss } from 'pg-boss';
import { v5 as uuidv5 } from 'uuid';
import { z } from 'zod';

import { identityFromKey } from '../apply/cdc-record.js';
import type { DeliveryMode } from '../apply/apply-engine.js';
import type { CdcEnvelope, DecodedCdcEvent } from '../cdc/cdc-handler.js';
import { CdcRuntime } from '../cdc/cdc-runtime.js';
import { required } from '../config.js';
import { KafkaCdcRouter } from '../kafka/kafka-cdc.router.js';

const queueName = 'cdc-events';
const deadLetterQueueName = 'cdc-events-dead-letter';

/** Fixed namespace for this consumer's job identities; any constant UUID works. */
const JOB_ID_NAMESPACE = '8f4a1e3c-6d2b-4f9a-9c1e-2b7d5a0f3e64';

const jobDataSchema = z.object({
  topic: z.string().min(1),
  partition: z.number().int().nonnegative(),
  offset: z.string().regex(/^\d+$/),
  messageId: z.string().min(1),
  schemaId: z.string().min(1),
  decoded: z.record(z.unknown()),
  rawBase64: z.string(),
  identity: z.string().nullable(),
  delivery: z.enum(['live', 'replay']),
});

export type KafkaCoordinates = {
  topic: string;
  partition: number;
  offset: string;
};

/**
 * Durable boundary owned by this service.
 *
 * Kafka decoding happens before enqueue because pg-boss stores JSONB, but no
 * table-specific Zod schema runs here. Once `send()` commits, Kafka may commit
 * its offset; handler/schema failures are now pg-boss retries and cannot stop a
 * Kafka partition.
 */
@Injectable()
export class CdcJobQueue implements OnApplicationShutdown {
  private readonly boss = new PgBoss(required('JOB_DATABASE_URL'));

  constructor(
    @Inject(CdcRuntime) private readonly runtime: CdcRuntime,
    @Inject(KafkaCdcRouter) private readonly router: KafkaCdcRouter,
  ) {}

  async start(): Promise<void> {
    this.boss.on('error', (error) => console.error('pg-boss:', error));
    await this.boss.start();
    await this.boss.createQueue(deadLetterQueueName, {
      retentionSeconds: 30 * 24 * 60 * 60,
      deleteAfterSeconds: 0,
    });
    await this.boss.createQueue(queueName, {
      policy: 'key_strict_fifo',
      retryLimit: Number(process.env.JOB_RETRY_LIMIT ?? 100),
      retryDelay: Number(process.env.JOB_RETRY_DELAY_SECONDS ?? 5),
      retryBackoff: true,
      retryDelayMax: Number(process.env.JOB_RETRY_DELAY_MAX_SECONDS ?? 1800),
      retentionSeconds: 4 * 24 * 60 * 60,
      deadLetter: deadLetterQueueName,
    });
    await this.boss.work(
      queueName,
      {
        localConcurrency: Number(process.env.JOB_CONCURRENCY ?? 20),
        pollingIntervalSeconds: 1,
      },
      async ([job]) => {
        if (!job) return;
        const data = jobDataSchema.parse(job.data);
        const envelope: CdcEnvelope = {
          topic: data.topic,
          messageId: data.messageId,
          schemaId: data.schemaId,
          decoded: data.decoded,
          raw: Buffer.from(data.rawBase64, 'base64'),
        };
        await this.router.process(envelope, data.identity, data.delivery);
      },
    );
  }

  async enqueue(
    coordinates: KafkaCoordinates,
    raw: Buffer,
    key: Buffer | null,
    delivery: DeliveryMode,
    replayId?: string,
  ): Promise<void> {
    if (delivery === 'replay' && !replayId) {
      throw new Error('Replay enqueue requires a replayId');
    }
    const messageId = kafkaMessageId(coordinates);
    const envelope = await this.runtime.decode(
      coordinates.topic,
      raw,
      messageId,
    );
    const identity = key
      ? identityFromKey(await this.runtime.decodeKey(coordinates.topic, key))
      : null;
    const decoded = jsonValue(envelope.decoded) as DecodedCdcEvent;

    await this.boss.send(
      queueName,
      {
        ...coordinates,
        messageId,
        schemaId: envelope.schemaId,
        decoded,
        rawBase64: raw.toString('base64'),
        identity,
        delivery,
      },
      {
        // Live deduplicates the Kafka coordinate forever while the job exists.
        // Each explicit replay gets a fresh namespace so it can deliberately
        // reapply LSN == watermark, while retries inside that replay dedupe.
        id: deterministicJobId(
          delivery === 'replay' ? `${replayId}:${messageId}` : messageId,
        ),
        // pg-boss serializes options through PostgreSQL JSONB, which rejects
        // the NUL separator often used for in-memory compound keys.
        singletonKey: JSON.stringify([
          coordinates.topic,
          identity ?? messageId,
        ]),
      },
    );
    // A null result means this exact Kafka coordinate was already enqueued.
    // That is success: it covers a crash after PostgreSQL commit but before the
    // Kafka offset commit without creating a duplicate job.
  }

  async onApplicationShutdown(): Promise<void> {
    await this.boss.stop({ graceful: true, timeout: 30_000 });
  }
}

function kafkaMessageId({ topic, partition, offset }: KafkaCoordinates): string {
  return `${topic}[${partition}]@${offset}`;
}

/**
 * pg-boss keys jobs by `uuid`, so the deduplication identity has to be encoded
 * as one. UUIDv5 is exactly that: a name hashed into a UUID inside a namespace.
 */
function deterministicJobId(name: string): string {
  return uuidv5(name, JOB_ID_NAMESPACE);
}

/** JSONB-safe copy: Avro longs are bigint and logical timestamps are Date. */
function jsonValue(value: unknown): unknown {
  return JSON.parse(
    JSON.stringify(value, (_key, item: unknown) =>
      typeof item === 'bigint' ? item.toString() : item,
    ),
  ) as unknown;
}
