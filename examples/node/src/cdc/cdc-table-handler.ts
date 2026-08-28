import { z } from 'zod';

import type {
  CdcHandler,
  CdcHandlerContext,
  DecodedCdcEvent,
} from './cdc-handler.js';
import { cdcSourceOf } from './cdc-handler.js';

/** CDC envelope around a row schema; extra fields pass through. */
export function cdcEnvelope<TRow extends z.ZodTypeAny>(row: TRow) {
  return z
    .object({
      op: z.enum(['c', 'u', 'd', 'r']),
      before: row.nullish(),
      after: row.nullish(),
      source: z
        .object({
          table: z.string(),
          schema: z.string().optional(),
          db: z.string().optional(),
        })
        .passthrough(),
    })
    .passthrough();
}

export type CdcTableEvent<TRow extends z.ZodTypeAny> = z.infer<
  ReturnType<typeof cdcEnvelope<TRow>>
>;

export type CdcTableEventContext<TRow extends z.ZodTypeAny> = {
  schemaId: string;
  op: 'c' | 'u' | 'd' | 'r';
  /** `after` for c/u/r, `before` for d — the row this event is about. */
  row: z.infer<TRow> | undefined;
  event: CdcTableEvent<TRow>;
  raw: Buffer;
};

/**
 * Template for a single-source handler: declare `source` (`{db}.{schema}.{table}`)
 * and the row schema, get envelope parsing and the before/after pick for free.
 */
export abstract class CdcTableHandler<TRow extends z.ZodTypeAny>
  implements CdcHandler<CdcTableEvent<TRow>>
{
  abstract readonly id: string;
  abstract readonly source: string;
  protected abstract readonly row: TRow;

  private envelope?: ReturnType<typeof cdcEnvelope<TRow>>;

  get schema(): ReturnType<typeof cdcEnvelope<TRow>> {
    this.envelope ??= cdcEnvelope(this.row);
    return this.envelope;
  }

  accepts(event: DecodedCdcEvent): boolean {
    return cdcSourceOf(event) === this.source;
  }

  async handle({
    schemaId,
    event,
    raw,
  }: CdcHandlerContext<CdcTableEvent<TRow>>): Promise<void> {
    await this.onEvent({
      schemaId,
      op: event.op,
      row: event.after ?? event.before ?? undefined,
      event,
      raw,
    });
  }

  protected abstract onEvent(ctx: CdcTableEventContext<TRow>): Promise<void>;
}
