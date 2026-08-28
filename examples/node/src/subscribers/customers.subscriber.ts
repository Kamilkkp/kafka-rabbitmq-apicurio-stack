import { CdcTableSubscriber } from '../cdc/cdc-table-subscriber.js';
import { customerHandler } from '../handlers/customers.handler.js';
import { genericOpLogHandler } from '../handlers/generic-op-log.handler.js';
import { KafkaSubscriber } from '../kafka/kafka-subscriber.decorator.js';

@KafkaSubscriber({ sources: ['sales.public.customers'] })
export class CustomersSubscriber extends CdcTableSubscriber {
  protected readonly handlers = [customerHandler, genericOpLogHandler];
}
