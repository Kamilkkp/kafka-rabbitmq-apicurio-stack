import { z } from 'zod';

import {
  CdcTableHandler,
  type CdcTableEventContext,
} from '../cdc/cdc-table-handler.js';

const orderRow = z
  .object({
    id: z.string(),
    customer_id: z.string(),
    status: z.string(),
    total_cents: z.number(),
    created_at: z.date(),
  })
  .passthrough();

class OrderHandler extends CdcTableHandler<typeof orderRow> {
  readonly id = 'sales-orders-v1';
  readonly source = 'sales.public.orders';
  protected readonly row = orderRow;

  protected async onEvent({
    schemaId,
    op,
    row,
  }: CdcTableEventContext<typeof orderRow>): Promise<void> {
    console.log(
      `[${this.id}] schemaId=${schemaId} op=${op} id=${row?.id ?? '?'} status=${row?.status ?? '?'} total_cents=${row?.total_cents ?? '?'}`,
    );
  }
}

export const orderHandler = new OrderHandler();
