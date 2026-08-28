import {
  MessageHandlerErrorBehavior,
  RabbitSubscribe,
} from '@golevelup/nestjs-rabbitmq';
import { Injectable } from '@nestjs/common';
import type { ConsumeMessage } from 'amqplib';

import { rabbitExchange, rabbitQueuePrefix } from '../config.js';

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
    errorBehavior: MessageHandlerErrorBehavior.REQUEUE,
  };
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
