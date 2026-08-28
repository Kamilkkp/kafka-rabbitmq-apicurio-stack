import { z } from 'zod';

import {
  CdcTableHandler,
  type CdcTableEventContext,
} from '../cdc/cdc-table-handler.js';

const productRow = z
  .object({
    id: z.string(),
    sku: z.string(),
    name: z.string(),
    unit: z.string(),
  })
  .passthrough();

class ProductHandler extends CdcTableHandler<typeof productRow> {
  readonly id = 'warehouse-products-v1';
  readonly source = 'warehouse.public.products';
  protected readonly row = productRow;

  protected async onEvent({
    schemaId,
    op,
    row,
  }: CdcTableEventContext<typeof productRow>): Promise<void> {
    console.log(
      `[${this.id}] schemaId=${schemaId} op=${op} sku=${row?.sku ?? '?'} name=${row?.name ?? '?'}`,
    );
  }
}

export const productHandler = new ProductHandler();
