import type { CdcRecord } from './cdc-record.js';
import { recordId } from './cdc-record.js';

/** Seed customer from `postgres/sales/init/02_seed.sql`. */
export const ADA_CUSTOMER_ID = '11111111-1111-4111-8111-111111111111';

export type Follow = {
  customerId: string;
  userId: string;
  note: string;
};

/**
 * In-memory projection of the demo sales tables plus one **local** table
 * (`follows`) that is never in CDC. A real app would keep user progress here.
 * A customer delete cascades into `follows`, mirroring the FK a real schema
 * would declare.
 */
export class ReadModel {
  readonly customers = new Map<string, Record<string, unknown>>();
  readonly orders = new Map<string, Record<string, unknown>>();
  readonly orderItems = new Map<string, Record<string, unknown>>();
  readonly products = new Map<string, Record<string, unknown>>();
  readonly warehouses = new Map<string, Record<string, unknown>>();
  readonly stockLevels = new Map<string, Record<string, unknown>>();
  readonly follows = new Map<string, Follow>();

  seedLocalProgress(): void {
    this.follows.set(ADA_CUSTOMER_ID, {
      customerId: ADA_CUSTOMER_ID,
      userId: 'local-user-1',
      note: 'watching Ada',
    });
  }

  table(source: string): Map<string, Record<string, unknown>> | undefined {
    switch (source) {
      case 'sales.public.customers':
        return this.customers;
      case 'sales.public.orders':
        return this.orders;
      case 'sales.public.order_items':
        return this.orderItems;
      case 'warehouse.public.products':
        return this.products;
      case 'warehouse.public.warehouses':
        return this.warehouses;
      case 'warehouse.public.stock_levels':
        return this.stockLevels;
      default:
        return undefined;
    }
  }

  upsert(record: CdcRecord): void {
    const table = this.table(record.source);
    if (!table || !record.after) {
      return;
    }
    table.set(record.key, record.after);
  }

  delete(record: CdcRecord): void {
    const table = this.table(record.source);
    table?.delete(record.key);
    if (record.source === 'sales.public.customers') {
      this.follows.delete(record.key);
    }
  }

  snapshot() {
    return {
      customers: this.customers.size,
      orders: this.orders.size,
      orderItems: this.orderItems.size,
      products: this.products.size,
      warehouses: this.warehouses.size,
      stockLevels: this.stockLevels.size,
      follows: this.follows.size,
    };
  }
}

export class WatermarkStore {
  private readonly lsn = new Map<string, bigint>();

  /**
   * Live delivery accepts only a strictly newer LSN. Replay also accepts an
   * equal LSN so rebuilding the projection really executes the latest event
   * again even when its watermark survived.
   */
  shouldApply(record: CdcRecord, replay: boolean): boolean {
    const prev = this.lsn.get(recordId(record));
    if (prev === undefined) {
      return true;
    }
    return replay ? record.lsn >= prev : record.lsn > prev;
  }

  recordApplied(record: CdcRecord): void {
    this.lsn.set(recordId(record), record.lsn);
  }
}
