import {
  RabbitSubscribe,
} from '@golevelup/nestjs-rabbitmq';
import { Injectable } from '@nestjs/common';
import type { ConfirmChannel, ConsumeMessage, Options } from 'amqplib';

import { rabbitExchange, rabbitQueuePrefix } from '../config.js';
import {
  deadLetterQueueName,
  retryDecision,
  retryHeaders,
  retryQueueName,
} from './retry-policy.js';

export interface RabbitMessageHandler {
  handle(payload: Buffer, message: ConsumeMessage): Promise<void>;
}

type RabbitSubscribeConfig = Parameters<typeof RabbitSubscribe>[0];

/**
 * Class-level `@RabbitSubscribe`: binds the subscription to `handle()`, which
 * the type parameter forces the class to expose.
 */
export function RabbitSubscriber(config: RabbitSubscribeConfig) {
  return <T extends new (...args: never[]) => RabbitMessageHandler>(
    target: T,
  ): T => {
    RabbitSubscribe(config)(target.prototype, 'handle', ownHandle(target));
    Injectable()(target);
    return target;
  };
}

/** Routing key is `{database}.{schema}.{table}`; one durable queue per source. */
export function cdcTableSubscription(source: string): RabbitSubscribeConfig {
  return {
    exchange: rabbitExchange,
    routingKey: source,
    queue: `${rabbitQueuePrefix}.${source}`,
    queueOptions: { durable: true },
    deserializer: (message) => message,
    errorHandler: async (channel, message, error) => {
      const decision = retryDecision(message.properties.headers);
      const queue =
        decision.kind === 'retry'
          ? retryQueueName(source, decision.delayMs)
          : deadLetterQueueName(source);

      try {
        await sendConfirmed(channel as ConfirmChannel, queue, message, {
          headers: retryHeaders(message.properties.headers, decision),
          persistent: true,
        });
        channel.ack(message);
        console.error(
          decision.kind === 'retry'
            ? `CDC ${source} failed; retry ${decision.attempt} in ${decision.delayMs}ms`
            : `CDC ${source} failed; parked after ${decision.attempt} attempts (${decision.reason})`,
          error,
        );
      } catch (publishError) {
        // Never acknowledge the original until its replacement is confirmed.
        // An infrastructure failure may spin briefly, but cannot lose the event.
        channel.nack(message, false, true);
        console.error(`Could not enqueue failed CDC ${source}`, publishError);
      }
    },
  };
}

function sendConfirmed(
  channel: ConfirmChannel,
  queue: string,
  message: ConsumeMessage,
  overrides: Options.Publish,
): Promise<void> {
  const { expiration: _expiration, headers, ...properties } = message.properties;
  return new Promise((resolve, reject) => {
    channel.sendToQueue(
      queue,
      message.content,
      { ...properties, ...overrides, headers: overrides.headers ?? headers },
      (error) => (error ? reject(error) : resolve()),
    );
  });
}

/**
 * `SetMetadata` writes onto the method function itself, so an inherited
 * `handle` would share one config across every subclass. Give each class its
 * own copy first.
 */
function ownHandle(target: new (...args: never[]) => RabbitMessageHandler) {
  const own = Object.getOwnPropertyDescriptor(target.prototype, 'handle');
  if (own) {
    return own;
  }

  const inherited: unknown = target.prototype.handle;
  if (typeof inherited !== 'function') {
    throw new Error(`${target.name} has no handle() method to subscribe`);
  }

  const handler = inherited as (...args: unknown[]) => Promise<void>;
  Object.defineProperty(target.prototype, 'handle', {
    value: function handle(this: unknown, ...args: unknown[]): Promise<void> {
      return handler.apply(this, args);
    },
    writable: true,
    configurable: true,
  });
  return Object.getOwnPropertyDescriptor(target.prototype, 'handle')!;
}
