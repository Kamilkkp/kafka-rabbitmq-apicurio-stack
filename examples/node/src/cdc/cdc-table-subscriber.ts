import { Inject } from '@nestjs/common';
import type { ConsumeMessage } from 'amqplib';

import { ApplyEngine } from '../apply/apply-engine.js';
import { ReplayCoordinator } from '../apply/replay-coordinator.js';
import { identityFromKey, toCdcRecord } from '../apply/cdc-record.js';
import { topicForRoutingKey } from '../config.js';
import { ReplayGate } from '../kafka/replay-gate.js';
import { CdcRuntime } from './cdc-runtime.js';
import type { CdcEnvelope, CdcHandler } from './cdc-handler.js';

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

  @Inject(ApplyEngine)
  protected readonly apply!: ApplyEngine;

  @Inject(ReplayCoordinator)
  protected readonly replay!: ReplayCoordinator;

  /** Try chain for this table. Narrow Zod schemas first, generic last. */
  protected abstract readonly handlers: CdcHandler[];

  /** RabbitMQ entry point, bound by `@RabbitSubscriber`. */
  async handle(payload: Buffer, message: ConsumeMessage): Promise<void> {
    await this.gate.waitUntilOpen();

    const { exchange, routingKey, deliveryTag } = message.fields;
    const headers = message.properties.headers;
    const replayId = headerString(headers, 'cdc-replay-id');
    const replayDelivery = headerString(headers, 'cdc-mode') === 'replay';
    if (replayDelivery && replayId && headerBoolean(headers, 'cdc-replay-end')) {
      this.replay.markFinished(replayId, routingKey);
      return;
    }

    const topic =
      headerString(headers, 'kafka-topic') ??
      topicForRoutingKey(routingKey);
    const envelope = await this.runtime.decode(
      topic,
      payload,
      `${exchange}:${routingKey}@${deliveryTag}`,
    );
    const record = toCdcRecord(
      envelope.decoded,
      await this.rowIdentity(topic, headers),
    );
    if (!record) {
      // No row identity or no LSN means the LWW gate cannot run, so say so
      // rather than letting the event through unnoticed.
      console.warn(
        `no CDC record built for ${envelope.messageId}; applied without LWW`,
      );
      await this.consume(envelope);
      return;
    }

    // Handlers run inside the gate: if one throws, the watermark does not move
    // and the message is nacked, so the redelivery retries this same event.
    const decision = await this.apply.apply(
      record,
      replayDelivery ? 'replay' : 'live',
      () => this.consume(envelope),
    );
    if (!decision.applied) {
      console.log(
        `skipped stale ${replayDelivery ? 'replay' : 'live'} ` +
          `${envelope.messageId} ${record.source}/${record.key} lsn=${record.lsn}`,
      );
    }
  }

  private async rowIdentity(
    topic: string,
    headers: ConsumeMessage['properties']['headers'],
  ): Promise<string | null | undefined> {
    const encoded = headerString(headers, 'kafka-key');
    if (!encoded) {
      return undefined;
    }
    const raw = Buffer.from(encoded, 'base64');
    if (raw.length === 0) {
      return undefined;
    }
    return identityFromKey(await this.runtime.decodeKey(topic, raw));
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

function headerBoolean(
  headers: ConsumeMessage['properties']['headers'],
  name: string,
): boolean {
  const value = headers?.[name];
  return value === true || value === 1 || value === 'true';
}
