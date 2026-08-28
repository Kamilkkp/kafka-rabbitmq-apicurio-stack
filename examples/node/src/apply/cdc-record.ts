import { z } from 'zod';

import type { DecodedCdcEvent } from '../cdc/cdc-handler.js';

export type CdcOp = 'c' | 'u' | 'd' | 'r';

export type CdcRecord = {
  source: string;
  key: string;
  op: CdcOp;
  after: Record<string, unknown> | null;
  before: Record<string, unknown> | null;
  lsn: bigint;
};

export function recordId(record: Pick<CdcRecord, 'source' | 'key'>): string {
  return `${record.source}\u0000${record.key}`;
}

/**
 * Row identity from a decoded Kafka message key. Debezium puts exactly the
 * primary key (or the `REPLICA IDENTITY` columns) there, so this needs no
 * per-table configuration and matches the key Kafka compacts on.
 */
export function identityFromKey(key: Record<string, unknown>): string | null {
  const names = Object.keys(key).sort();
  if (names.length === 0) {
    return null;
  }
  if (names.length === 1) {
    return String(key[names[0]]);
  }
  return names.map((name) => `${name}=${String(key[name])}`).join('|');
}

/**
 * Last-resort identity when the message key is unavailable: assume the `id`
 * convention. A table with a composite key has no usable identity here — every
 * column would join the key, so an update would look like a different row.
 */
export function rowKey(row: Record<string, unknown>): string | null {
  return row.id != null ? String(row.id) : null;
}

/**
 * Avro `long` initially decodes to bigint (see `SafeLong`). pg-boss stores the
 * generic decoded envelope as JSONB, so the worker receives the same value as
 * a decimal string.
 */
const avroLong = z.union([
  z.bigint(),
  z.number().int().transform(BigInt),
  z.string().regex(/^-?\d+$/).transform(BigInt),
]);

const rowSchema = z.record(z.unknown());

/**
 * The slice of the Debezium envelope this projection needs, declared instead of
 * probed. Everything else in the payload belongs to the table handlers.
 *
 * `op` is one of the row operations; `m` (message) and `t` (truncate) carry no
 * row and are rejected here.
 */
const debeziumEnvelope = z.object({
  op: z.enum(['c', 'u', 'd', 'r']),
  before: rowSchema.nullish(),
  after: rowSchema.nullish(),
  source: z.object({
    db: z.string().min(1).optional(),
    schema: z.string().min(1).optional(),
    table: z.string().min(1),
    lsn: avroLong.nullish(),
    ts_ms: avroLong.nullish(),
  }),
});

/**
 * `identity` is the decoded Kafka message key. Pass it whenever the transport
 * carried it; without it the `id` convention is the only fallback.
 */
export function toCdcRecord(
  decoded: DecodedCdcEvent,
  identity?: string | null,
): CdcRecord | null {
  const parsed = debeziumEnvelope.safeParse(decoded);
  if (!parsed.success) {
    return null;
  }

  const { op, before, after, source } = parsed.data;
  const row = after ?? before;
  if (!row) {
    return null;
  }

  const key = identity ?? rowKey(row);
  if (key == null) {
    return null;
  }

  return {
    source: [source.db, source.schema, source.table]
      .filter((part): part is string => part != null)
      .join('.'),
    key,
    op,
    after: after ?? null,
    before: before ?? null,
    // The WAL position orders events for a row. A snapshot read can omit it,
    // so fall back to the source timestamp.
    lsn: source.lsn ?? source.ts_ms ?? 0n,
  };
}
