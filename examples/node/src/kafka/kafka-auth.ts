import { required } from '../config.js';

/**
 * librdkafka auth for the advertised client listeners.
 * The Compose stack requires SCRAM on localhost:9092; PLAINTEXT is only the
 * unadvertised broker-internal bootstrap used by kafka-auth-init.
 */
export function kafkaAuthConfig(): Record<string, string> {
  const protocol = process.env.KAFKA_SECURITY_PROTOCOL ?? 'SASL_PLAINTEXT';
  if (protocol === 'PLAINTEXT') {
    return {};
  }

  return {
    'security.protocol': protocol,
    'sasl.mechanisms': process.env.KAFKA_SASL_MECHANISM ?? 'SCRAM-SHA-512',
    'sasl.username': required('KAFKA_SASL_USERNAME'),
    'sasl.password': required('KAFKA_SASL_PASSWORD'),
  };
}
