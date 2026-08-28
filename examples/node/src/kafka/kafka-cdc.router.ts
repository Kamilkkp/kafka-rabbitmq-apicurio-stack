import { Inject, Injectable, type OnModuleInit } from '@nestjs/common';
import { DiscoveryService } from '@nestjs/core';

import { cdcSourceOf, type CdcEnvelope } from '../cdc/cdc-handler.js';
import { CdcRuntime } from '../cdc/cdc-runtime.js';
import { CdcTableSubscriber } from '../cdc/cdc-table-subscriber.js';
import {
  KAFKA_CDC_TABLE,
  type KafkaSubscriberConfig,
} from './kafka-subscriber.decorator.js';

/**
 * Kafka counterpart of the RabbitMQ routing key: routes by `{db}.{table}` into
 * the same `@KafkaSubscriber` classes, so a replay after `seek` to the
 * beginning is handled exactly like a live message.
 */
@Injectable()
export class KafkaCdcRouter implements OnModuleInit {
  private readonly bySource = new Map<string, CdcTableSubscriber>();

  // Explicit tokens: tsx strips types without emitting `design:paramtypes`.
  constructor(
    @Inject(DiscoveryService) private readonly discovery: DiscoveryService,
    @Inject(CdcRuntime) private readonly runtime: CdcRuntime,
  ) {}

  onModuleInit(): void {
    for (const wrapper of this.discovery.getProviders()) {
      const { instance, metatype } = wrapper;
      if (!(instance instanceof CdcTableSubscriber) || !metatype) {
        continue;
      }
      const config = Reflect.getOwnMetadata(KAFKA_CDC_TABLE, metatype) as
        | KafkaSubscriberConfig
        | undefined;
      if (config) {
        for (const source of config.sources) {
          this.bySource.set(source, instance);
        }
      }
    }
  }

  sources(): string[] {
    return [...this.bySource.keys()];
  }

  async route(topic: string, raw: Buffer, messageId: string): Promise<void> {
    await this.deliver(await this.runtime.decode(topic, raw, messageId));
  }

  /** Already decoded — used after the apply engine has accepted the record. */
  async deliver(envelope: CdcEnvelope): Promise<void> {
    const source = cdcSourceOf(envelope.decoded);
    const subscriber = source ? this.bySource.get(source) : undefined;
    if (!subscriber) {
      await this.runtime.dispatch(envelope);
      return;
    }
    await subscriber.consume(envelope);
  }
}
