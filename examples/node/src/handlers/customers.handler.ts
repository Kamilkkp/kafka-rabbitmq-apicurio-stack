import { z } from 'zod';

import {
  CdcTableHandler,
  type CdcTableEventContext,
} from '../cdc/cdc-table-handler.js';

const customerRow = z
  .object({
    id: z.string(),
    email: z.string(),
    full_name: z.string(),
    // pg-boss JSONB preserves the instant as ISO text.
    created_at: z.coerce.date(),
  })
  .passthrough();

class CustomerHandler extends CdcTableHandler<typeof customerRow> {
  readonly id = 'sales-customers-v1';
  readonly source = 'sales.public.customers';
  protected readonly row = customerRow;

  protected async onEvent({
    schemaId,
    op,
    row,
  }: CdcTableEventContext<typeof customerRow>): Promise<void> {
    console.log(
      `[${this.id}] schemaId=${schemaId} op=${op} id=${row?.id ?? '?'} email=${row?.email ?? '?'} name=${row?.full_name ?? '?'}`,
    );
  }
}

export const customerHandler = new CustomerHandler();
