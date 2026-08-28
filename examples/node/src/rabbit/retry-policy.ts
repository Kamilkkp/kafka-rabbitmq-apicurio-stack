import type { ConsumeMessage } from 'amqplib';

export const retryDelaysMs = [5_000, 30_000, 300_000, 1_800_000] as const;
export const retryMaxAttempts = Number(
  process.env.CDC_RABBITMQ_RETRY_MAX_ATTEMPTS ?? 100,
);
export const retryMaxAgeMs = Number(
  process.env.CDC_RABBITMQ_RETRY_MAX_AGE_MS ?? 3 * 24 * 60 * 60 * 1_000,
);

const attemptHeader = 'cdc-retry-attempt';
const firstFailureHeader = 'cdc-first-failure-ms';

export type RetryDecision =
  | {
      kind: 'retry';
      attempt: number;
      firstFailureMs: number;
      delayMs: (typeof retryDelaysMs)[number];
    }
  | {
      kind: 'dead-letter';
      attempt: number;
      firstFailureMs: number;
      reason: 'attempt-limit' | 'age-limit';
    };

export function retryDecision(
  headers: ConsumeMessage['properties']['headers'],
  nowMs = Date.now(),
): RetryDecision {
  const attempt = integerHeader(headers?.[attemptHeader]) + 1;
  const firstFailureMs = integerHeader(headers?.[firstFailureHeader]) || nowMs;

  if (attempt >= retryMaxAttempts) {
    return { kind: 'dead-letter', attempt, firstFailureMs, reason: 'attempt-limit' };
  }
  if (nowMs - firstFailureMs >= retryMaxAgeMs) {
    return { kind: 'dead-letter', attempt, firstFailureMs, reason: 'age-limit' };
  }

  return {
    kind: 'retry',
    attempt,
    firstFailureMs,
    delayMs: delayForAttempt(attempt),
  };
}

export function retryHeaders(
  headers: ConsumeMessage['properties']['headers'],
  decision: RetryDecision,
): Record<string, unknown> {
  return {
    ...headers,
    [attemptHeader]: decision.attempt,
    [firstFailureHeader]: decision.firstFailureMs,
    ...(decision.kind === 'dead-letter'
      ? { 'cdc-dead-letter-reason': decision.reason }
      : {}),
  };
}

export function retryQueueName(source: string, delayMs: number): string {
  return `${process.env.CDC_RABBITMQ_QUEUE ?? 'cdc-consumer'}.retry.${delayMs}.${source}`;
}

export function deadLetterQueueName(source: string): string {
  return `${process.env.CDC_RABBITMQ_QUEUE ?? 'cdc-consumer'}.dlq.${source}`;
}

function delayForAttempt(attempt: number): (typeof retryDelaysMs)[number] {
  if (attempt === 1) return retryDelaysMs[0];
  if (attempt <= 5) return retryDelaysMs[1];
  if (attempt <= 15) return retryDelaysMs[2];
  return retryDelaysMs[3];
}

function integerHeader(value: unknown): number {
  if (typeof value === 'number' && Number.isSafeInteger(value) && value >= 0) {
    return value;
  }
  if (typeof value === 'string' && /^\d+$/.test(value)) {
    return Number(value);
  }
  if (Buffer.isBuffer(value) && /^\d+$/.test(value.toString())) {
    return Number(value.toString());
  }
  return 0;
}
