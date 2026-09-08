import { Module } from '@nestjs/common';
import { DiscoveryModule } from '@nestjs/core';

import { ApplyModule } from './apply/apply.module.js';
import { CdcRuntime } from './cdc/cdc-runtime.js';
import { required } from './config.js';
import { CdcJobQueue } from './job/cdc-job.queue.js';
import { KafkaCdcRouter } from './kafka/kafka-cdc.router.js';
import { KafkaConsumer } from './kafka/kafka-consumer.js';
import { KafkaReplay } from './kafka/kafka-replay.js';
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
  ],
  providers: [
    {
      provide: CdcRuntime,
      useFactory: () => new CdcRuntime(required('SCHEMA_REGISTRY_URL')),
    },
    KafkaCdcRouter,
    KafkaConsumer,
    KafkaReplay,
    CdcJobQueue,
    CustomersSubscriber,
    OrdersSubscriber,
    ProductsSubscriber,
    OtherTablesSubscriber,
    WarehousesSubscriber,
    StockLevelsSubscriber,
  ],
})
export class AppModule {}
