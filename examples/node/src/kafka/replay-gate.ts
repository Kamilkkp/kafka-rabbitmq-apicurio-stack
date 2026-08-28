import { Injectable } from '@nestjs/common';

import { kafkaReplayOnStart } from '../config.js';

/**
 * Holds RabbitMQ delivery until the startup Kafka replay finishes.
 * Duplicate events on the overlap are the handlers' problem (idempotency).
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
