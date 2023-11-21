import { Semaphore } from '@cubejs-backend/shared';
import { QueryCache } from './QueryCache';

export abstract class AbstractSetMemoryQueue {
  protected readonly queue: Set<string> = new Set();

  protected readonly executionSem: Semaphore;

  protected readonly addSem: Semaphore;

  public constructor(
    protected readonly capacity: number,
    concurrency: number,
  ) {
    this.executionSem = new Semaphore(concurrency);
    this.addSem = new Semaphore(capacity);
  }

  protected execution: boolean = false;

  public async addToQueue(item: string) {
    const next = this.addSem.acquire();
    this.queue.add(item);

    if (this.queue.size > this.capacity) {
      await this.onCapacity();
    }

    this.run().catch(e => console.log(e));
    await next;
  }

  public async run(): Promise<void> {
    if (this.execution) {
      return;
    }

    this.execution = true;

    try {
      while (this.queue.size) {
        const toExecute = this.queue[Symbol.iterator]().next().value;
        if (toExecute) {
          this.queue.delete(toExecute);
          await this.executionSem.acquire();

          this.execute(toExecute).finally(() => {
            this.executionSem.release();
            this.addSem.release();
          });
        }
      }
    } finally {
      this.execution = false;
    }
  }

  protected abstract onCapacity(): Promise<void>;

  protected abstract execute(item: string): Promise<void>;
}

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

  protected async onCapacity(): Promise<void> {
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

export class TableUsedMemoryQueue extends AbstractSetMemoryQueue {
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

  protected async onCapacity(): Promise<void> {
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
      this.logger('TableUsedMemoryQueue not enough capacity', {
        message: `TableUsedMemoryQueue reached max capacity: ${this.capacity}. Please reduce number of pre-aggregations by using higher granularity.`
      });
    }
  }

  protected async execute(tableName: string): Promise<void> {
    const key = this.queryCache.getKey('SQL_PRE_AGGREGATIONS_TABLES_USED', tableName);

    try {
      await this.queryCache.getCacheDriver().set(key, true, this.touchTablePersistTime);
    } catch (e: any) {
      this.logger('Error on pre-aggregation used update', {
        error: (e.stack || e),
        key
      });
    }
  }
}
