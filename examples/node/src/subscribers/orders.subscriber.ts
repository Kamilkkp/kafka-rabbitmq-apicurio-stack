import { CdcTableSubscriber } from '../cdc/cdc-table-subscriber.js';
import { genericOpLogHandler } from '../handlers/generic-op-log.handler.js';
import { orderHandler } from '../handlers/orders.handler.js';
import { KafkaSubscriber } from '../kafka/kafka-subscriber.decorator.js';
import {
  cdcTableSubscription,
  RabbitSubscriber,
} from '../rabbit/rabbit-subscriber.decorator.js';

@RabbitSubscriber(cdcTableSubscription('sales.public.orders'))
@KafkaSubscriber({ sources: ['sales.public.orders'] })
export class OrdersSubscriber extends CdcTableSubscriber {
  protected readonly handlers = [orderHandler, genericOpLogHandler];
}
