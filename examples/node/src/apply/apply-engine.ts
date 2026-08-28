import type { CdcRecord } from './cdc-record.js';
import { recordId } from './cdc-record.js';
import { ReadModel, WatermarkStore } from './read-model.js';

export type DeliveryMode = 'replay' | 'live';

export type ApplyDecision =
  | { applied: false; reason: 'stale' }
  | { applied: true; kind: 'upsert' | 'delete' };

/**
 * Last-write-wins projection keyed by `(source, pk)`. The only question asked
 * per event is "was a newer version of this row already applied".
 *
 * - **live**: only a strictly newer `source.lsn` is applied, so a Rabbit
 *   redelivery or an out-of-order event is dropped.
 * - **replay**: an LSN *equal* to the watermark is applied again, so rebuilding
 *   from Kafka always re-executes the latest known event for every row. An
 *   older LSN is still dropped.
 *
 * Deletes execute where they appear, in both modes.
 *
 * The watermark advances **only after the write succeeded**. That is what makes
 * "stale" safe to acknowledge: the watermark is a record of applied writes, not
 * of seen messages. If the write throws, the watermark stays put, so the
 * redelivery is not stale and the event is attempted again — which is how a
 * child row waits for its parent without deferring foreign keys.
 *
 * Because the write sits between reading and moving the watermark, the gate is
 * check-then-act and needs the row to itself. Work is therefore serialized per
 * `(source, pk)` — different rows still run concurrently, so a prefetch above 1
 * buys real parallelism. The SQL equivalent is a single conditional upsert
 * (`... WHERE excluded.lsn > projection.lsn`), which the database makes atomic
 * for you; a row lock held for the transaction does the same job.
 */
export class ApplyEngine {
  readonly model = new ReadModel();
  private readonly watermarks = new WatermarkStore();
  private readonly rowQueues = new Map<string, Promise<unknown>>();

  constructor() {
    this.model.seedLocalProgress();
  }

  /**
   * `write` is the rest of the unit of work (handlers, SQL). It runs inside the
   * LWW gate so the ordering cannot be got wrong by the caller. On SQL, put the
   * projection write and the watermark update in one transaction.
   */
  apply(
    record: CdcRecord,
    delivery: DeliveryMode = 'live',
    write?: () => Promise<void>,
  ): Promise<ApplyDecision> {
    return this.perRow(recordId(record), () =>
      this.applyToRow(record, delivery, write),
    );
  }

  private async applyToRow(
    record: CdcRecord,
    delivery: DeliveryMode,
    write?: () => Promise<void>,
  ): Promise<ApplyDecision> {
    if (!this.watermarks.shouldApply(record, delivery === 'replay')) {
      return { applied: false, reason: 'stale' };
    }

    if (record.op === 'd') {
      this.model.delete(record);
    } else {
      this.model.upsert(record);
    }

    await write?.();

    this.watermarks.recordApplied(record);
    return { applied: true, kind: record.op === 'd' ? 'delete' : 'upsert' };
  }

  /** Chains `task` behind whatever is already running for this row. */
  private perRow<T>(id: string, task: () => Promise<T>): Promise<T> {
    const previous = this.rowQueues.get(id) ?? Promise.resolve();
    // Both arms run `task`: a failed predecessor must not cancel its successor.
    const current = previous.then(task, task);
    this.rowQueues.set(id, current);

    void current
      .catch(() => undefined)
      .then(() => {
        // Drop the chain once the row goes idle, or the map grows per row seen.
        if (this.rowQueues.get(id) === current) {
          this.rowQueues.delete(id);
        }
      });

    return current;
  }
}
