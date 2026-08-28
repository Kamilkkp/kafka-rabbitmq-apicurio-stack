import { CdcTableSubscriber } from '../cdc/cdc-table-subscriber.js';
import { customerHandler } from '../handlers/customers.handler.js';
import { genericOpLogHandler } from '../handlers/generic-op-log.handler.js';
import { KafkaSubscriber } from '../kafka/kafka-subscriber.decorator.js';
import {
  cdcTableSubscription,
  RabbitSubscriber,
} from '../rabbit/rabbit-subscriber.decorator.js';

@RabbitSubscriber(cdcTableSubscription('sales.public.customers'))
@KafkaSubscriber({ sources: ['sales.public.customers'] })
export class CustomersSubscriber extends CdcTableSubscriber {
  protected readonly handlers = [customerHandler, genericOpLogHandler];
}
