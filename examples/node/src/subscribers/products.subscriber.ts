import { CdcTableSubscriber } from '../cdc/cdc-table-subscriber.js';
import { genericOpLogHandler } from '../handlers/generic-op-log.handler.js';
import { productHandler } from '../handlers/products.handler.js';
import { KafkaSubscriber } from '../kafka/kafka-subscriber.decorator.js';
import {
  cdcTableSubscription,
  RabbitSubscriber,
} from '../rabbit/rabbit-subscriber.decorator.js';

@RabbitSubscriber(cdcTableSubscription('warehouse.public.products'))
@KafkaSubscriber({ sources: ['warehouse.public.products'] })
export class ProductsSubscriber extends CdcTableSubscriber {
  protected readonly handlers = [productHandler, genericOpLogHandler];
}
