import { Injectable } from '@nestjs/common';

import { kafkaReplayOnStart } from '../config.js';

/**
 * Holds RabbitMQ delivery until the startup Kafka replay finishes.
 * Overlap with the bridge is dropped by the apply engine (LWW on `source.lsn`).
 */
@Injectable()
export class ReplayGate {
  private readonly opened: Promise<void>;
  private release: () => void = () => undefined;

  constructor() {
    this.opened = kafkaReplayOnStart
      ? new Promise<void>((resolve) => (this.release = resolve))
      : Promise.resolve();
  }

  finish(): void {
    this.release();
  }

  async waitUntilOpen(): Promise<void> {
    await this.opened;
  }
}
