import { Inject } from '@nestjs/common';

import { ApplyEngine } from '../apply/apply-engine.js';
import { toCdcRecord } from '../apply/cdc-record.js';
import type { DeliveryMode } from '../apply/apply-engine.js';
import { CdcRuntime } from './cdc-runtime.js';
import type { CdcEnvelope, CdcHandler } from './cdc-handler.js';

/**
 * One processor per CDC source (`{database}.{schema}.{table}`). Kafka only
 * decodes and durably enqueues a generic envelope; pg-boss invokes this class,
 * so table-specific Zod schemas cannot hold a Kafka partition open.
 */
export abstract class CdcTableSubscriber {
  // Property injection: subclasses stay constructor-free, so Nest cannot lose
  // the dependency through an unemitted `design:paramtypes`.
  @Inject(CdcRuntime)
  protected readonly runtime!: CdcRuntime;

  @Inject(ApplyEngine)
  protected readonly apply!: ApplyEngine;

  /** Try chain for this table. Narrow Zod schemas first, generic last. */
  protected abstract readonly handlers: CdcHandler[];

  /** pg-boss entry point. */
  async process(
    envelope: CdcEnvelope,
    identity: string | null,
    delivery: DeliveryMode,
  ): Promise<void> {
    const record = toCdcRecord(
      envelope.decoded,
      identity,
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
      delivery,
      () => this.consume(envelope),
    );
    if (!decision.applied) {
      console.log(
        `skipped stale ${delivery} ` +
          `${envelope.messageId} ${record.source}/${record.key} lsn=${record.lsn}`,
      );
    }
  }

  /** The table-specific Zod dispatch, deliberately behind the durable queue. */
  async consume(envelope: CdcEnvelope): Promise<void> {
    await this.runtime.dispatch(envelope, this.handlers);
  }
}
