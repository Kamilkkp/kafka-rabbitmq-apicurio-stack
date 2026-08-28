import { Inject, Injectable } from '@nestjs/common';

import { ApplyEngine } from './apply-engine.js';

/**
 * Tracks how far a replay has got. The publisher emits one end marker per
 * table topic; the replay is done once every queue has consumed its marker.
 *
 * This is observability, not a correctness gate: LWW is decided per event from
 * the `cdc-mode` header, so nothing waits for the markers.
 *
 * A marker can therefore overtake events still being processed, and the logged
 * snapshot may miss the last few rows. Nothing is allowed to depend on it —
 * buying back that guarantee would cost either prefetch=1 or a barrier per
 * queue, and LWW makes the guarantee worthless anyway.
 */
@Injectable()
export class ReplayCoordinator {
  private replayId?: string;
  private expected = new Set<string>();
  private finished = new Set<string>();

  constructor(@Inject(ApplyEngine) private readonly engine: ApplyEngine) {}

  begin(replayId: string, sources: string[]): void {
    if (this.replayId) {
      throw new Error(`Replay ${this.replayId} is already running`);
    }
    this.replayId = replayId;
    this.expected = new Set(sources);
    this.finished.clear();

    if (this.expected.size === 0) {
      this.complete();
    }
  }

  markFinished(replayId: string, source: string): void {
    if (replayId !== this.replayId || !this.expected.has(source)) {
      return;
    }
    this.finished.add(source);
    if (this.finished.size === this.expected.size) {
      this.complete();
    }
  }

  private complete(): void {
    console.log(
      `replay ${this.replayId ?? '(empty)'} applied via RabbitMQ ` +
        `snapshot=${JSON.stringify(this.engine.model.snapshot())}`,
    );
    this.replayId = undefined;
    this.expected.clear();
    this.finished.clear();
  }
}
