import { z } from 'zod';

import type { CdcHandler } from '../cdc/cdc-handler.js';
import { cdcSourceOf } from '../cdc/cdc-handler.js';

/**
 * Last-resort handler: any Debezium envelope with an `op` string.
 * Keep last in the chain so typed handlers get first chance.
 */
export const genericOpLogEventSchema = z
  .object({
    op: z.string().min(1),
    after: z.any().optional(),
  })
  .passthrough();

export type GenericOpLogEvent = z.infer<typeof genericOpLogEventSchema>;

export const genericOpLogHandler: CdcHandler<GenericOpLogEvent> = {
  id: 'generic-op-log',
  schema: genericOpLogEventSchema,
  async handle({ schemaId, event }) {
    console.log(
      `[generic-op-log] schemaId=${schemaId} op=${event.op} source=${cdcSourceOf(event) ?? '?'}`,
    );
  },
};
