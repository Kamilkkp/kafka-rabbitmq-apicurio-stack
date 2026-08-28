import avro from 'avsc';

/** Debezium / Kafka Connect timestamp as epoch millis → Date. */
class TimestampMillisLogicalType extends avro.types.LogicalType {
  _fromValue(val: number | bigint): Date {
    return new Date(typeof val === 'bigint' ? Number(val) : val);
  }

  _toValue(date: Date): number {
    return date.getTime();
  }

  _resolve(type: avro.Type): unknown {
    if (avro.Type.isType(type, 'long', 'int', 'string')) {
      return this._fromValue.bind(this);
    }
    return undefined;
  }
}

/** Debezium ZonedTimestamp (`timestamptz`) as ISO-8601 string → Date. */
class ZonedTimestampLogicalType extends avro.types.LogicalType {
  _fromValue(val: string): Date {
    return new Date(val);
  }

  _toValue(date: Date): string {
    return date.toISOString();
  }

  _resolve(type: avro.Type): unknown {
    if (avro.Type.isType(type, 'string')) {
      return this._fromValue.bind(this);
    }
    return undefined;
  }
}

/** Debezium MicroTimestamp as epoch micros → Date. */
class TimestampMicrosLogicalType extends avro.types.LogicalType {
  _fromValue(val: number | bigint): Date {
    const micros = typeof val === 'bigint' ? val : BigInt(val);
    return new Date(Number(micros / 1000n));
  }

  _toValue(date: Date): bigint {
    return BigInt(date.getTime()) * 1000n;
  }

  _resolve(type: avro.Type): unknown {
    if (avro.Type.isType(type, 'long', 'int', 'string')) {
      return this._fromValue.bind(this);
    }
    return undefined;
  }
}

/**
 * LSN / txId / plain longs can exceed Number.MAX_SAFE_INTEGER.
 * `noUnpack=false` ⇒ avsc gives an 8-byte LE buffer after zigzag unpack.
 */
export const SafeLong = avro.types.LongType.__with(
  {
    fromBuffer: (buf: Buffer) => buf.readBigInt64LE(0),
    toBuffer: (n: bigint) => {
      const out = Buffer.alloc(8);
      out.writeBigInt64LE(n);
      return out;
    },
    fromJSON: BigInt,
    toJSON: (n: bigint) => n.toString(),
    isValid: (n: unknown): n is bigint => typeof n === 'bigint',
    compare: (a: bigint, b: bigint) => (a === b ? 0 : a < b ? -1 : 1),
  },
  false,
);

const ZONED_TIMESTAMP = 'debezium-zoned-timestamp';

/** Debezium `connect.name` → logical type, keyed by the underlying Avro type. */
const DEBEZIUM_TIME_TO_LOGICAL: Record<string, Record<string, string>> = {
  long: {
    'io.debezium.time.Timestamp': 'timestamp-millis',
    'io.debezium.time.MicroTimestamp': 'timestamp-micros',
    'io.debezium.time.NanoTimestamp': 'timestamp-micros',
  },
  string: {
    'io.debezium.time.ZonedTimestamp': ZONED_TIMESTAMP,
  },
};

/**
 * avsc typeHook: stamp Debezium connect.name times as Avro logicalTypes (→ Date).
 * Return undefined so default Type construction continues on the mutated schema.
 */
export function debeziumTimeTypeHook(schema: unknown): avro.Type | undefined {
  if (!schema || typeof schema !== 'object' || Array.isArray(schema)) {
    return undefined;
  }
  const rec = schema as Record<string, unknown>;
  if (typeof rec.type !== 'string' || typeof rec['connect.name'] !== 'string') {
    return undefined;
  }
  const logical = DEBEZIUM_TIME_TO_LOGICAL[rec.type]?.[rec['connect.name']];
  if (logical && !rec.logicalType) {
    rec.logicalType = logical;
  }
  return undefined;
}

/**
 * Options for avsc / `@confluentinc/schemaregistry` `AvroSerdeConfig`.
 *
 * A factory, not a constant: avsc writes every named type it builds into
 * `registry`, and the serde registers referenced schemas (`event.block`,
 * `Source`) into it. Sharing one object across schemas that reference the same
 * types throws `duplicate type name`, so each deserializer needs its own.
 */
export function debeziumAvroTypeOptions() {
  return {
    registry: { long: SafeLong },
    logicalTypes: {
      'timestamp-millis': TimestampMillisLogicalType,
      'timestamp-micros': TimestampMicrosLogicalType,
      [ZONED_TIMESTAMP]: ZonedTimestampLogicalType,
    },
    typeHook: debeziumTimeTypeHook,
  };
}
