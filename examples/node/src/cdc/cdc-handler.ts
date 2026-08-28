import type { z } from 'zod';

export type DecodedCdcEvent = Record<string, unknown>;

/** One decoded CDC message, whatever transport delivered it. */
export type CdcEnvelope = {
  /** Kafka topic that owns the Avro subject (`<topic>-value`). */
  topic: string;
  messageId: string;
  raw: Buffer;
  schemaId: string;
  decoded: DecodedCdcEvent;
};

/** `{database}.{schema}.{table}`, matching the RabbitMQ routing key. */
export function cdcSourceOf(decoded: DecodedCdcEvent): string | undefined {
  const source = decoded.source;
  if (!source || typeof source !== 'object') {
    return undefined;
  }
  const table = (source as { table?: unknown }).table;
  const schema = (source as { schema?: unknown }).schema;
  const db = (source as { db?: unknown }).db;
  if (typeof table !== 'string') {
    return undefined;
  }
  const parts = [
    typeof db === 'string' && db.length > 0 ? db : undefined,
    typeof schema === 'string' && schema.length > 0 ? schema : undefined,
    table,
  ].filter((part): part is string => part != null);
  return parts.join('.');
}

export type CdcHandlerContext<T> = {
  schemaId: string;
  event: T;
  raw: Buffer;
};

export interface CdcHandler<T = unknown> {
  readonly id: string;
  /**
   * Shape this handler accepts (Zod = “schema” for dispatch). Input stays
   * `unknown` so any schema producing `T` fits, whatever it parses from.
   */
  readonly schema: z.ZodType<T, z.ZodTypeDef, unknown>;
  /**
   * Optional cheap filter before Zod (e.g. source.table).
   * Skip without counting as a failed parse attempt when useful.
   */
  accepts?(event: DecodedCdcEvent): boolean;
  handle(ctx: CdcHandlerContext<T>): Promise<void>;
}

export class UnhandledCdcEventError extends Error {
  constructor(
    readonly schemaId: string,
    readonly triedHandlerIds: string[],
  ) {
    super(
      `No CDC handler matched payload for writer schema id=${schemaId} (tried: ${triedHandlerIds.join(', ') || 'none'})`,
    );
    this.name = 'UnhandledCdcEventError';
  }
}
