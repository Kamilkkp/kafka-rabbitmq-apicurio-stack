import { CdcTableSubscriber } from '../cdc/cdc-table-subscriber.js';
import { genericOpLogHandler } from '../handlers/generic-op-log.handler.js';
import { KafkaSubscriber } from '../kafka/kafka-subscriber.decorator.js';

@KafkaSubscriber({ sources: ['sales.public.order_items'] })
export class OtherTablesSubscriber extends CdcTableSubscriber {
  protected readonly handlers = [genericOpLogHandler];
}

@KafkaSubscriber({ sources: ['warehouse.public.warehouses'] })
export class WarehousesSubscriber extends CdcTableSubscriber {
  protected readonly handlers = [genericOpLogHandler];
}

@KafkaSubscriber({ sources: ['warehouse.public.stock_levels'] })
export class StockLevelsSubscriber extends CdcTableSubscriber {
  protected readonly handlers = [genericOpLogHandler];
}
