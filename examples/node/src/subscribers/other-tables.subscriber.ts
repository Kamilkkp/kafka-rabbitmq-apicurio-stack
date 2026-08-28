import { CdcTableSubscriber } from '../cdc/cdc-table-subscriber.js';
import { genericOpLogHandler } from '../handlers/generic-op-log.handler.js';
import { KafkaSubscriber } from '../kafka/kafka-subscriber.decorator.js';
import {
  cdcTableSubscription,
  RabbitSubscriber,
} from '../rabbit/rabbit-subscriber.decorator.js';

const otherSources = [
  'sales.public.order_items',
  'warehouse.public.warehouses',
  'warehouse.public.stock_levels',
] as const;

@RabbitSubscriber(cdcTableSubscription(otherSources[0]))
@KafkaSubscriber({ sources: [...otherSources] })
export class OtherTablesSubscriber extends CdcTableSubscriber {
  protected readonly handlers = [genericOpLogHandler];
}

@RabbitSubscriber(cdcTableSubscription('warehouse.public.warehouses'))
export class WarehousesSubscriber extends CdcTableSubscriber {
  protected readonly handlers = [genericOpLogHandler];
}

@RabbitSubscriber(cdcTableSubscription('warehouse.public.stock_levels'))
export class StockLevelsSubscriber extends CdcTableSubscriber {
  protected readonly handlers = [genericOpLogHandler];
}
