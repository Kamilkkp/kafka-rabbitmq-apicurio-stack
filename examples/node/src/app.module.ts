import { RabbitMQModule } from '@golevelup/nestjs-rabbitmq';
import { Module } from '@nestjs/common';
import { DiscoveryModule } from '@nestjs/core';

import { ApplyModule } from './apply/apply.module.js';
import { CdcRuntime } from './cdc/cdc-runtime.js';
import { exampleCdcSources, rabbitExchange, required } from './config.js';
import { KafkaCdcRouter } from './kafka/kafka-cdc.router.js';
import { KafkaReplay } from './kafka/kafka-replay.js';
import { ReplayGate } from './kafka/replay-gate.js';
import {
  deadLetterQueueName,
  retryDelaysMs,
  retryQueueName,
} from './rabbit/retry-policy.js';
import { CustomersSubscriber } from './subscribers/customers.subscriber.js';
import { OrdersSubscriber } from './subscribers/orders.subscriber.js';
import {
  OtherTablesSubscriber,
  StockLevelsSubscriber,
  WarehousesSubscriber,
} from './subscribers/other-tables.subscriber.js';
import { ProductsSubscriber } from './subscribers/products.subscriber.js';

@Module({
  imports: [
    ApplyModule,
    DiscoveryModule,
    RabbitMQModule.forRoot({
      uri: required('CDC_RABBITMQ_URL'),
      exchanges: [
        {
          name: rabbitExchange,
          type: 'topic',
          options: { durable: true },
        },
      ],
      queues: exampleCdcSources.flatMap((source) => [
        ...retryDelaysMs.map((delayMs) => ({
          name: retryQueueName(source, delayMs),
          options: {
            durable: true,
            messageTtl: delayMs,
            deadLetterExchange: rabbitExchange,
            deadLetterRoutingKey: source,
          },
        })),
        {
          // Intentionally has no consumer. Operators inspect and redrive it.
          name: deadLetterQueueName(source),
          options: { durable: true },
        },
      ]),
      // No ordering is required: LWW decides per row and the apply engine
      // serializes same-row work, so messages may be processed concurrently.
      prefetchCount: Number(process.env.CDC_RABBITMQ_PREFETCH ?? 20),
      connectionInitOptions: {
        wait: true,
        timeout: 10_000,
      },
    }),
  ],
  providers: [
    {
      provide: CdcRuntime,
      useFactory: () => new CdcRuntime(required('CDC_APICURIO_URL')),
    },
    KafkaCdcRouter,
    KafkaReplay,
    ReplayGate,
    CustomersSubscriber,
    OrdersSubscriber,
    ProductsSubscriber,
    OtherTablesSubscriber,
    WarehousesSubscriber,
    StockLevelsSubscriber,
  ],
})
export class AppModule {}
