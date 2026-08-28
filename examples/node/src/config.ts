export function required(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(`Missing env ${name}`);
  }
  return value;
}

/** Explicit replay topics; otherwise replay uses this service's processors. */
export const cdcTopicOverride = process.env.CDC_TOPICS
  ? uniqueTopics(process.env.CDC_TOPICS.split(','))
  : undefined;

/**
 * Replay every compacted Kafka table topic once at startup, then hand over
 * to the regular Kafka consumer. Off by default.
 */
export const kafkaReplayOnStart = process.env.CDC_KAFKA_REPLAY === 'true';

function uniqueTopics(topics: string[]): string[] {
  return [...new Set(topics.map((topic) => topic.trim()).filter(Boolean))];
}
