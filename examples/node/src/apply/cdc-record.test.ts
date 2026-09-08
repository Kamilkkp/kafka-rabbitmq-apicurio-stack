import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { identityFromKey, toCdcRecord } from './cdc-record.js';

/** Shape the Confluent Avro decoder produces: Avro longs arrive as bigint. */
function envelope(overrides: Record<string, unknown> = {}) {
  return {
    op: 'u',
    before: null,
    after: { id: 7, email: 'ada@example.test' },
    ts_ms: 1_724_846_400_000n,
    source: {
      version: '3.0.0.Final',
      connector: 'postgresql',
      db: 'sales',
      schema: 'public',
      table: 'customers',
      lsn: 23_461_952n,
      ts_ms: 1_724_846_400_000n,
    },
    ...overrides,
  };
}

describe('toCdcRecord', () => {
  it('extracts source, key, op and LSN from a decoded envelope', () => {
    const record = toCdcRecord(envelope());

    assert.ok(record);
    assert.equal(record.source, 'sales.public.customers');
    assert.equal(record.key, '7');
    assert.equal(record.op, 'u');
    assert.equal(record.lsn, 23_461_952n);
    assert.deepEqual(record.after, { id: 7, email: 'ada@example.test' });
  });

  it('keys a delete off the before image', () => {
    const record = toCdcRecord(
      envelope({ op: 'd', after: null, before: { id: 7 } }),
    );

    assert.ok(record);
    assert.equal(record.key, '7');
    assert.equal(record.after, null);
  });

  it('falls back to source.ts_ms when a snapshot read has no LSN', () => {
    const record = toCdcRecord(
      envelope({
        op: 'r',
        source: { ...envelope().source, lsn: null },
      }),
    );

    assert.ok(record);
    assert.equal(record.lsn, 1_724_846_400_000n);
  });

  it('restores an LSN serialized through pg-boss JSONB', () => {
    const record = toCdcRecord(
      envelope({
        source: { ...envelope().source, lsn: '23461952' },
      }),
    );

    assert.ok(record);
    assert.equal(record.lsn, 23_461_952n);
  });

  it('uses the decoded Kafka key for a composite primary key', () => {
    const identity = identityFromKey({
      warehouse_id: 'w1',
      product_id: 'p2',
    });
    const record = toCdcRecord(
      envelope({
        after: { warehouse_id: 'w1', product_id: 'p2', quantity: 5 },
      }),
      identity,
    );

    assert.ok(record);
    assert.equal(record.key, 'product_id=p2|warehouse_id=w1');
  });

  it('rejects a row without id when its Kafka key is unavailable', () => {
    const record = toCdcRecord(
      envelope({
        after: { warehouse_id: 'w1', product_id: 'p2', quantity: 5 },
      }),
    );

    assert.equal(record, null);
  });

  it('rejects an envelope with no table', () => {
    const { table: _table, ...source } = envelope().source;

    assert.equal(toCdcRecord(envelope({ source })), null);
  });

  it('rejects a non-row operation', () => {
    assert.equal(toCdcRecord(envelope({ op: 't' })), null);
  });

  it('rejects an event carrying neither before nor after', () => {
    assert.equal(toCdcRecord(envelope({ before: null, after: null })), null);
  });
});
