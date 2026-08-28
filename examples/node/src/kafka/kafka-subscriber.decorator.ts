import { Injectable, SetMetadata } from '@nestjs/common';

import type { CdcTableSubscriber } from '../cdc/cdc-table-subscriber.js';

export const KAFKA_CDC_TABLE = 'cdc:kafka-table';

export type KafkaSubscriberConfig = {
  /** Matched against `{source.db}.{source.schema}.{source.table}`. */
  sources: string[];
};

/**
 * Marks a processor as the owner of one or more Kafka CDC sources.
 * `KafkaCdcRouter` picks it up at boot for pg-boss job dispatch.
 */
export function KafkaSubscriber(config: KafkaSubscriberConfig) {
  return <T extends new (...args: never[]) => CdcTableSubscriber>(
    target: T,
  ): T => {
    SetMetadata(KAFKA_CDC_TABLE, config)(target);
    Injectable()(target);
    return target;
  };
}
