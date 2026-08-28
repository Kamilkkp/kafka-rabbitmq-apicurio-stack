import type { CdcEnvelope, CdcHandler } from './cdc-handler.js';
import { UnhandledCdcEventError } from './cdc-handler.js';

/**
 * Cache Zod match results per handler + writer schema id.
 * Same Avro schema id ⇒ same decoded shape ⇒ failed handlers stay skipped.
 */
export class ZodMatchCache {
  private readonly cache = new Map<string, boolean>();

  get(handlerId: string, schemaId: string): boolean | undefined {
    return this.cache.get(`${handlerId}:${schemaId}`);
  }

  set(handlerId: string, schemaId: string, matched: boolean): void {
    this.cache.set(`${handlerId}:${schemaId}`, matched);
  }

  size(): number {
    return this.cache.size;
  }
}

export async function dispatchCdcMessage(
  envelope: CdcEnvelope,
  handlers: CdcHandler[],
  matchCache: ZodMatchCache = new ZodMatchCache(),
): Promise<{ handlerId: string; event: unknown }> {
  const { schemaId, decoded, raw } = envelope;
  const tried: string[] = [];

  for (const handler of handlers) {
    if (handler.accepts && !handler.accepts(decoded)) {
      continue;
    }

    tried.push(handler.id);

    if (matchCache.get(handler.id, schemaId) === false) {
      continue;
    }

    const result = handler.schema.safeParse(decoded);
    matchCache.set(handler.id, schemaId, result.success);
    if (!result.success) {
      continue;
    }

    await handler.handle({ schemaId, event: result.data, raw });
    return { handlerId: handler.id, event: result.data };
  }

  throw new UnhandledCdcEventError(schemaId, tried);
}
