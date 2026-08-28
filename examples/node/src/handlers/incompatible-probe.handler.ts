import { z } from 'zod';

import type { CdcHandler } from '../cdc/cdc-handler.js';

/** Always fails Zod — exercises chain skip. */
export const incompatibleProbeHandler: CdcHandler = {
  id: 'incompatible-probe',
  schema: z.object({
    thisFieldWillNeverExistOnDebeziumEvents: z.number(),
  }),
  async handle() {
    throw new Error('incompatible-probe must never handle a real CDC event');
  },
};
