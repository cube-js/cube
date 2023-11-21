import { AbstractSetMemoryQueue } from '@cubejs-backend/shared';
import { QueryCache } from './QueryCache';

export class TableTouchMemoryQueue extends AbstractSetMemoryQueue {
  public constructor(
    capacity: number,
    concurrency: number,
    protected readonly logger: any,
    protected readonly queryCache: QueryCache,
    protected readonly touchTablePersistTime: number
  ) {
    super(capacity, concurrency);
  }

  protected lastWarningDate: Date | null = null;

  protected onCapacity(): void {
    let showWarning = false;

    if (this.lastWarningDate) {
      const now = new Date();
      const diffInMS = now.getTime() - this.lastWarningDate.getTime();

      if (diffInMS > 60 * 1000) {
        showWarning = true;
        this.lastWarningDate = now;
      }
    } else {
      showWarning = true;
    }

    if (showWarning) {
      this.logger('TableTouchMemoryQueue not enough capacity', {
        message: `TableTouchMemoryQueue reached max capacity: ${this.capacity}. Please reduce number of pre-aggregations by using higher granularity.`
      });
    }
  }

  protected async execute(tableName: string): Promise<void> {
    const key = this.queryCache.getKey('SQL_PRE_AGGREGATIONS_TABLES_TOUCH', tableName);

    try {
      await this.queryCache.getCacheDriver().set(key, new Date().getTime(), this.touchTablePersistTime);
    } catch (e: any) {
      this.logger('Error on pre-aggregation touch update', {
        error: (e.stack || e),
        key
      });
    }
  }
}

