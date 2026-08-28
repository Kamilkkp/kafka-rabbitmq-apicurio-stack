import { Inject } from '@nestjs/common';
import type { ConsumeMessage } from 'amqplib';

import { CdcRuntime } from './cdc-runtime.js';
import type { CdcEnvelope, CdcHandler } from './cdc-handler.js';
import { topicForRoutingKey } from '../config.js';
import { ReplayGate } from '../kafka/replay-gate.js';

/**
 * One subscriber per CDC source (`{database}.{schema}.{table}`). Transports
 * (RabbitMQ routing key, Kafka `source.db` + `schema` + `table`) pick the class;
 * decoding and dispatch live here so both paths run the exact same code.
 */
export abstract class CdcTableSubscriber {
  // Property injection: subclasses stay constructor-free, so Nest cannot lose
  // the dependency through an unemitted `design:paramtypes`.
  @Inject(CdcRuntime)
  protected readonly runtime!: CdcRuntime;

  @Inject(ReplayGate)
  protected readonly gate!: ReplayGate;

  /** Try chain for this table. Narrow Zod schemas first, generic last. */
  protected abstract readonly handlers: CdcHandler[];

  /** RabbitMQ entry point, bound by `@RabbitSubscriber`. */
  async handle(payload: Buffer, message: ConsumeMessage): Promise<void> {
    await this.gate.waitUntilOpen();

    const { exchange, routingKey, deliveryTag } = message.fields;
    const topic =
      headerString(message.properties.headers, 'kafka_topic') ??
      headerString(message.properties.headers, 'kafka-topic') ??
      topicForRoutingKey(routingKey);
    const envelope = await this.runtime.decode(
      topic,
      payload,
      `${exchange}:${routingKey}@${deliveryTag}`,
    );
    await this.consume(envelope);
  }

  /** Kafka entry point: the router decoded already to find this subscriber. */
  async consume(envelope: CdcEnvelope): Promise<void> {
    await this.runtime.dispatch(envelope, this.handlers);
  }
}

function headerString(
  headers: ConsumeMessage['properties']['headers'],
  name: string,
): string | undefined {
  const value = headers?.[name];
  if (typeof value === 'string' && value.length > 0) {
    return value;
  }
  if (Buffer.isBuffer(value) && value.length > 0) {
    return value.toString();
  }
  return undefined;
}
