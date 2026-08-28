import {
  AvroDeserializer,
  SchemaId,
  SchemaRegistryClient,
  SerdeType,
  SubjectNameStrategyType,
} from '@confluentinc/schemaregistry';

import { debeziumAvroTypeOptions } from './avro-logical-types.js';

/**
 * One deserializer per writer schema, plus the first-decode lock.
 *
 * The very first decode of a schema is exclusive: the serde fetches referenced
 * schemas before caching the built type, so concurrent Kafka partition reads
 * would both build it and the loser would hit
 * `duplicate type name` on the shared avsc registry.
 */
class DeserializerCache {
  private readonly deserializers = new Map<string, AvroDeserializer>();
  private readonly priming = new Map<string, Promise<void>>();

  constructor(
    private readonly client: SchemaRegistryClient,
    private readonly serdeType: SerdeType,
  ) {}

  create(): AvroDeserializer {
    return new AvroDeserializer(this.client, this.serdeType, {
      ...debeziumAvroTypeOptions(),
      // Default ASSOCIATED strategy calls a Confluent-only endpoint Apicurio
      // lacks. TOPIC matches Debezium's TopicIdStrategy: `<topic>-value` and
      // `<topic>-key`.
      subjectNameStrategyType: SubjectNameStrategyType.TOPIC,
    });
  }

  async decode(
    topic: string,
    buffer: Buffer,
    schemaId: string,
  ): Promise<unknown> {
    let deserializer = this.deserializers.get(schemaId);
    if (deserializer == null) {
      deserializer = this.create();
      this.deserializers.set(schemaId, deserializer);
    }

    const priming = this.priming.get(schemaId);
    if (priming == null) {
      const first = deserializer.deserialize(topic, buffer);
      this.priming.set(
        schemaId,
        // A failed prime must not leave the schema marked as built.
        first.then(
          () => undefined,
          () => {
            this.priming.delete(schemaId);
          },
        ),
      );
      return await first;
    }

    await priming;
    return await deserializer.deserialize(topic, buffer);
  }
}

/**
 * Decode Confluent-wire Avro via Apicurio **ccompat** (`@confluentinc/schemaregistry`,
 * the client Confluent ships with `@confluentinc/kafka-javascript`).
 *
 * Requires Debezium `apicurio.registry.artifact.group-id=default` so nested
 * Value/Source refs resolve (Apicurio #5133). Longs → bigint, Debezium times → Date
 * via avsc options (SafeLong + logicalTypes) passed as `AvroSerdeConfig`.
 */
export class ApicurioAvroDecoder {
  private readonly values: DeserializerCache;
  private readonly keys: DeserializerCache;
  private readonly schemaIdReader: AvroDeserializer;

  constructor(registryBaseUrl: string) {
    const client = new SchemaRegistryClient({
      baseURLs: [`${registryBaseUrl.replace(/\/$/, '')}/apis/ccompat/v7`],
    });
    this.values = new DeserializerCache(client, SerdeType.VALUE);
    this.keys = new DeserializerCache(client, SerdeType.KEY);
    this.schemaIdReader = this.values.create();
  }

  /**
   * Topic drives subject lookup for referenced schemas; the id still comes from
   * the payload.
   */
  async decode(
    topic: string,
    buffer: Buffer,
  ): Promise<Record<string, unknown>> {
    const decoded = await this.values.decode(
      topic,
      buffer,
      this.schemaIdOf(topic, buffer),
    );
    return decoded as Record<string, unknown>;
  }

  /**
   * Decode a Kafka message key against `<topic>-key`. For Debezium this is the
   * row identity: the primary key, or the columns `REPLICA IDENTITY` names.
   */
  async decodeKey(
    topic: string,
    buffer: Buffer,
  ): Promise<Record<string, unknown>> {
    const decoded = await this.keys.decode(
      topic,
      buffer,
      // Key and value schema ids come from different subjects and can collide,
      // so namespace the cache key.
      `key:${this.schemaIdOf(topic, buffer)}`,
    );
    return decoded as Record<string, unknown>;
  }

  /**
   * Writer schema identity, used as a cache key. Handles both wire formats:
   * magic 0 (4-byte id) and magic 1 (16-byte guid), plus id-in-headers.
   */
  schemaIdOf(topic: string, buffer: Buffer): string {
    const schemaId = new SchemaId('AVRO');
    this.schemaIdReader.deserializeSchemaId(topic, buffer, schemaId);
    const id = schemaId.id ?? schemaId.guid;
    if (id == null) {
      throw new Error(`No Avro schema id in payload for topic ${topic}`);
    }
    return String(id);
  }
}
