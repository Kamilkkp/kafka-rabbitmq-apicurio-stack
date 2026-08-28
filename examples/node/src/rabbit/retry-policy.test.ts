import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import {
  retryDecision,
  retryHeaders,
  retryMaxAgeMs,
} from './retry-policy.js';

describe('Rabbit retry policy', () => {
  it('backs off from seconds to thirty minutes', () => {
    assert.deepEqual(retryDecision(undefined, 1_000), {
      kind: 'retry',
      attempt: 1,
      firstFailureMs: 1_000,
      delayMs: 5_000,
    });
    const second = retryDecision({ 'cdc-retry-attempt': 1 }, 1_000);
    const sixteenth = retryDecision({ 'cdc-retry-attempt': 15 }, 1_000);
    assert.equal(second.kind === 'retry' && second.delayMs, 30_000);
    assert.equal(sixteenth.kind === 'retry' && sixteenth.delayMs, 1_800_000);
  });

  it('parks the hundredth failed delivery', () => {
    assert.deepEqual(
      retryDecision(
        {
          'cdc-retry-attempt': Buffer.from('99'),
          'cdc-first-failure-ms': '1000',
        },
        2_000,
      ),
      {
        kind: 'dead-letter',
        attempt: 100,
        firstFailureMs: 1_000,
        reason: 'attempt-limit',
      },
    );
  });

  it('parks after three days even below the attempt limit', () => {
    const decision = retryDecision(
      {
        'cdc-retry-attempt': 10,
        'cdc-first-failure-ms': 1_000,
      },
      1_000 + retryMaxAgeMs,
    );

    assert.equal(decision.kind, 'dead-letter');
    assert.equal(
      decision.kind === 'dead-letter' && decision.reason,
      'age-limit',
    );
  });

  it('preserves transport headers while recording retry state', () => {
    const decision = retryDecision(undefined, 123);
    assert.deepEqual(retryHeaders({ 'kafka-topic': 'sales.public.orders' }, decision), {
      'kafka-topic': 'sales.public.orders',
      'cdc-retry-attempt': 1,
      'cdc-first-failure-ms': 123,
    });
  });
});
