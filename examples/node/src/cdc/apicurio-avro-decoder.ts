import {
  AvroDeserializer,
  SchemaId,
  SchemaRegistryClient,
  SerdeType,
  SubjectNameStrategyType,
} from '@confluentinc/schemaregistry';

import { debeziumAvroTypeOptions } from './avro-logical-types.js';

/**
 * Decode Confluent-wire Avro via Apicurio **ccompat** (`@confluentinc/schemaregistry`,
 * the client Confluent ships with `@confluentinc/kafka-javascript`).
 *
 * Requires Debezium `apicurio.registry.artifact.group-id=default` so nested
 * Value/Source refs resolve (Apicurio #5133). Longs → bigint, Debezium times → Date
 * via avsc options (SafeLong + logicalTypes) passed as `AvroSerdeConfig`.
 */
export class ApicurioAvroDecoder {
  private readonly client: SchemaRegistryClient;
  /** One deserializer per writer schema; see `debeziumAvroTypeOptions`. */
  private readonly deserializers = new Map<string, AvroDeserializer>();
  /** In-flight first decode per schema; see `decode`. */
  private readonly priming = new Map<string, Promise<void>>();
  private readonly schemaIdReader: AvroDeserializer;

  constructor(registryBaseUrl: string) {
    this.client = new SchemaRegistryClient({
      baseURLs: [`${registryBaseUrl.replace(/\/$/, '')}/apis/ccompat/v7`],
    });
    this.schemaIdReader = this.createDeserializer();
  }

  private createDeserializer(): AvroDeserializer {
    return new AvroDeserializer(this.client, SerdeType.VALUE, {
      ...debeziumAvroTypeOptions(),
      // Default ASSOCIATED strategy calls a Confluent-only endpoint Apicurio lacks.
      // TOPIC matches Debezium's TopicIdStrategy: <topic>-value.
      subjectNameStrategyType: SubjectNameStrategyType.TOPIC,
    });
  }

  /**
   * Topic drives subject lookup for referenced schemas; the id still comes from
   * the payload.
   *
   * The very first decode of a schema is exclusive. The serde fetches the
   * referenced schemas before caching the built type, so concurrent decodes of
   * one schema (RabbitMQ prefetch > 1) would both build it and the loser would
   * hit `duplicate type name` on the shared avsc registry.
   */
  async decode(
    topic: string,
    buffer: Buffer,
  ): Promise<Record<string, unknown>> {
    const schemaId = this.schemaIdOf(topic, buffer);
    let deserializer = this.deserializers.get(schemaId);
    if (deserializer == null) {
      deserializer = this.createDeserializer();
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
      return (await first) as Record<string, unknown>;
    }

    await priming;
    return (await deserializer.deserialize(topic, buffer)) as Record<
      string,
      unknown
    >;
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
