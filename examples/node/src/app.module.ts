import { RabbitMQModule } from '@golevelup/nestjs-rabbitmq';
import { Module } from '@nestjs/common';
import { DiscoveryModule } from '@nestjs/core';

import { CdcRuntime } from './cdc/cdc-runtime.js';
import { rabbitExchange, required } from './config.js';
import { KafkaCdcRouter } from './kafka/kafka-cdc.router.js';
import { KafkaReplay } from './kafka/kafka-replay.js';
import { ReplayGate } from './kafka/replay-gate.js';
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
      prefetchCount: Number(process.env.CDC_RABBITMQ_PREFETCH ?? 10),
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
