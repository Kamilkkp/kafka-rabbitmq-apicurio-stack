import { ApicurioAvroDecoder } from './apicurio-avro-decoder.js';
import type {
  CdcEnvelope,
  CdcHandler,
  DecodedCdcEvent,
} from './cdc-handler.js';
import { UnhandledCdcEventError } from './cdc-handler.js';
import { dispatchCdcMessage, ZodMatchCache } from './dispatch.js';
import { customerHandler } from '../handlers/customers.handler.js';
import { genericOpLogHandler } from '../handlers/generic-op-log.handler.js';
import { incompatibleProbeHandler } from '../handlers/incompatible-probe.handler.js';
import { orderHandler } from '../handlers/orders.handler.js';
import { productHandler } from '../handlers/products.handler.js';

/** Order = try chain. Narrow Zod schemas first, generic last. */
export const defaultCdcHandlers: CdcHandler[] = [
  incompatibleProbeHandler,
  customerHandler,
  orderHandler,
  productHandler,
  genericOpLogHandler,
];

/** Avro longs decode to bigint, which JSON.stringify refuses to serialize. */
function jsonSafe(_key: string, value: unknown): unknown {
  return typeof value === 'bigint' ? value.toString() : value;
}

export class CdcRuntime {
  private readonly decoder: ApicurioAvroDecoder;
  private readonly matchCache = new ZodMatchCache();

  constructor(apicurioUrl: string) {
    this.decoder = new ApicurioAvroDecoder(apicurioUrl);
  }

  handlerIds(handlers: CdcHandler[] = defaultCdcHandlers): string[] {
    return handlers.map((handler) => handler.id);
  }

  async decode(
    topic: string,
    raw: Buffer,
    messageId: string,
  ): Promise<CdcEnvelope> {
    return {
      topic,
      messageId,
      raw,
      schemaId: this.decoder.schemaIdOf(topic, raw),
      decoded: (await this.decoder.decode(topic, raw)) as DecodedCdcEvent,
    };
  }

  async decodeKey(
    topic: string,
    raw: Buffer,
  ): Promise<Record<string, unknown>> {
    return await this.decoder.decodeKey(topic, raw);
  }

  /** Run an already decoded message through a handler chain. */
  async dispatch(
    envelope: CdcEnvelope,
    handlers: CdcHandler[] = defaultCdcHandlers,
  ): Promise<void> {
    try {
      const { handlerId, event } = await dispatchCdcMessage(
        envelope,
        handlers,
        this.matchCache,
      );
      const op =
        event && typeof event === 'object' && 'op' in event
          ? String((event as { op: unknown }).op)
          : '?';
      console.log(
        `processed ${envelope.messageId} via ${handlerId} (zodCache=${this.matchCache.size()}) op=${op}`,
      );
    } catch (err) {
      const error = err instanceof Error ? err : new Error(String(err));
      if (err instanceof UnhandledCdcEventError) {
        console.error(error.message);
        console.error(
          `decoded payload (schemaId=${err.schemaId}):\n${JSON.stringify(envelope.decoded, jsonSafe, 2)}`,
        );
      } else {
        console.error(`handle failed: ${error.message}`);
      }
      throw error;
    }
  }

  async handle(
    topic: string,
    raw: Buffer,
    messageId: string,
    handlers?: CdcHandler[],
  ): Promise<void> {
    await this.dispatch(await this.decode(topic, raw, messageId), handlers);
  }
}
