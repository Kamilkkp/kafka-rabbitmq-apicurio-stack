import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { ApplyEngine } from './apply-engine.js';
import type { CdcRecord } from './cdc-record.js';
import { ADA_CUSTOMER_ID } from './read-model.js';

function rec(
  partial: Pick<CdcRecord, 'op' | 'lsn'> &
    Partial<Pick<CdcRecord, 'source' | 'key' | 'after'>>,
): CdcRecord {
  const source = partial.source ?? 'sales.public.customers';
  const key = partial.key ?? ADA_CUSTOMER_ID;
  const after =
    partial.op === 'd'
      ? null
      : (partial.after ?? {
          id: key,
          email: 'ada@example.test',
          full_name: 'Ada',
        });
  return {
    source,
    key,
    op: partial.op,
    after,
    before: partial.op === 'd' ? { id: key } : null,
    lsn: partial.lsn,
  };
}

describe('ApplyEngine', () => {
  it('live: skips a stale event after a newer LSN was applied', async () => {
    const engine = new ApplyEngine();
    const first = await engine.apply(
      rec({ op: 'u', lsn: 100n, after: { id: ADA_CUSTOMER_ID, full_name: 'new' } }),
    );
    const stale = await engine.apply(
      rec({ op: 'u', lsn: 50n, after: { id: ADA_CUSTOMER_ID, full_name: 'old' } }),
    );

    assert.equal(first.applied, true);
    assert.equal(stale.applied, false);
    assert.equal(engine.model.customers.get(ADA_CUSTOMER_ID)?.full_name, 'new');
  });

  it('live: rejects a redelivery of the event already applied', async () => {
    const engine = new ApplyEngine();
    await engine.apply(rec({ op: 'u', lsn: 100n }));

    const redelivery = await engine.apply(rec({ op: 'u', lsn: 100n }));

    assert.equal(redelivery.applied, false);
  });

  it('replay: reapplies an event equal to the persisted watermark', async () => {
    const engine = new ApplyEngine();
    await engine.apply(
      rec({
        op: 'u',
        lsn: 100n,
        after: { id: ADA_CUSTOMER_ID, full_name: 'already applied' },
      }),
    );

    const equal = await engine.apply(
      rec({
        op: 'u',
        lsn: 100n,
        after: { id: ADA_CUSTOMER_ID, full_name: 'rebuilt by replay' },
      }),
      'replay',
    );

    assert.equal(equal.applied, true);
    assert.equal(
      engine.model.customers.get(ADA_CUSTOMER_ID)?.full_name,
      'rebuilt by replay',
    );
  });

  it('replay: rejects an event older than the persisted watermark', async () => {
    const engine = new ApplyEngine();
    await engine.apply(rec({ op: 'u', lsn: 100n }));

    const stale = await engine.apply(rec({ op: 'u', lsn: 99n }), 'replay');

    assert.equal(stale.applied, false);
  });

  it('applies a delete that is the newest event for the row', async () => {
    const engine = new ApplyEngine();
    await engine.apply(rec({ op: 'c', lsn: 1n }));

    await engine.apply(rec({ op: 'd', lsn: 2n }));

    assert.equal(engine.model.customers.has(ADA_CUSTOMER_ID), false);
    assert.equal(engine.model.follows.has(ADA_CUSTOMER_ID), false);
  });

  it('applies a delete seen mid-stream even if a later insert restores the row', async () => {
    const engine = new ApplyEngine();

    await engine.apply(rec({ op: 'd', lsn: 10n }), 'replay');
    await engine.apply(
      rec({ op: 'c', lsn: 20n, after: { id: ADA_CUSTOMER_ID, full_name: 'Ada' } }),
      'replay',
    );

    assert.equal(engine.model.customers.has(ADA_CUSTOMER_ID), true);
    // The synced row comes back; the cascaded local row does not.
    assert.equal(engine.model.follows.has(ADA_CUSTOMER_ID), false);
  });

  it('accepts a child row whose parent has not arrived yet', async () => {
    const engine = new ApplyEngine();
    const orderId = 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa';

    await engine.apply(
      rec({
        op: 'c',
        lsn: 5n,
        source: 'sales.public.orders',
        key: orderId,
        after: { id: orderId, customer_id: ADA_CUSTOMER_ID, status: 'paid' },
      }),
      'replay',
    );

    assert.equal(engine.model.orders.has(orderId), true);
  });

  it('does not advance the watermark when the write fails', async () => {
    const engine = new ApplyEngine();
    const event = rec({ op: 'u', lsn: 100n });

    await assert.rejects(
      engine.apply(event, 'live', () => Promise.reject(new Error('FK violation'))),
      /FK violation/,
    );

    // The redelivery must be attempted, not written off as already applied.
    let retried = false;
    const retry = await engine.apply(event, 'live', async () => {
      retried = true;
    });

    assert.equal(retry.applied, true);
    assert.equal(retried, true);
  });

  it('serializes concurrent events for one row so the newest LSN wins', async () => {
    const engine = new ApplyEngine();
    // The older event holds its write open, so with an unguarded gate the
    // newer one would read the same watermark and finish first.
    let releaseOlder: () => void = () => undefined;
    const olderWriting = new Promise<void>((resolve) => {
      releaseOlder = resolve;
    });

    const older = engine.apply(
      rec({ op: 'u', lsn: 10n, after: { id: ADA_CUSTOMER_ID, full_name: 'older' } }),
      'live',
      () => olderWriting,
    );
    const newer = engine.apply(
      rec({ op: 'u', lsn: 20n, after: { id: ADA_CUSTOMER_ID, full_name: 'newer' } }),
    );

    releaseOlder();
    assert.equal((await older).applied, true);
    assert.equal((await newer).applied, true);
    assert.equal(engine.model.customers.get(ADA_CUSTOMER_ID)?.full_name, 'newer');

    // The real damage of an interleaved gate is a watermark walked backwards:
    // the older event would record LSN 10 after the newer one recorded 20.
    const stale = await engine.apply(
      rec({ op: 'u', lsn: 15n, after: { id: ADA_CUSTOMER_ID, full_name: 'stale' } }),
    );
    assert.equal(stale.applied, false);
  });

  it('keeps the row queue usable after a failed write', async () => {
    const engine = new ApplyEngine();

    const failing = engine.apply(rec({ op: 'u', lsn: 10n }), 'live', () =>
      Promise.reject(new Error('FK violation')),
    );
    const following = engine.apply(
      rec({ op: 'u', lsn: 20n, after: { id: ADA_CUSTOMER_ID, full_name: 'next' } }),
    );

    await assert.rejects(failing, /FK violation/);
    assert.equal((await following).applied, true);
    assert.equal(engine.model.customers.get(ADA_CUSTOMER_ID)?.full_name, 'next');
  });

  it('stops retrying once the write has succeeded', async () => {
    const engine = new ApplyEngine();
    const event = rec({ op: 'u', lsn: 100n });
    let writes = 0;

    await engine.apply(event, 'live', async () => {
      writes += 1;
    });
    const redelivery = await engine.apply(event, 'live', async () => {
      writes += 1;
    });

    assert.equal(redelivery.applied, false);
    assert.equal(writes, 1);
  });
});
