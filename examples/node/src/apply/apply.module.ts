import { Global, Module } from '@nestjs/common';

import { ApplyEngine } from './apply-engine.js';
import { ReplayCoordinator } from './replay-coordinator.js';

@Global()
@Module({
  providers: [
    {
      provide: ApplyEngine,
      useFactory: () => new ApplyEngine(),
    },
    ReplayCoordinator,
  ],
  exports: [ApplyEngine, ReplayCoordinator],
})
export class ApplyModule {}
