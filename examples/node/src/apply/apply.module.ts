import { Global, Module } from '@nestjs/common';

import { ApplyEngine } from './apply-engine.js';

@Global()
@Module({
  providers: [
    {
      provide: ApplyEngine,
      useFactory: () => new ApplyEngine(),
    },
  ],
  exports: [ApplyEngine],
})
export class ApplyModule {}
