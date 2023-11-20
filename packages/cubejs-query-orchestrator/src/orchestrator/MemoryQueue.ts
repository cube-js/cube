import { QueryCache } from './QueryCache';
import { pausePromise, Semaphore } from '@cubejs-backend/shared';

export abstract class AbstractSetMemoryQueue {
  protected readonly queue: Set<string> = new Set();

  protected readonly execution_sem: Semaphore;

  protected readonly add_sem: Semaphore;

  public constructor(
    protected readonly capacity: number,
    concurrency: number,
  ) {
    this.execution_sem = new Semaphore(concurrency);
    this.add_sem = new Semaphore(capacity);
  }

  protected execution: boolean = false;

  public async addToQueue(item: string) {
    const next = this.add_sem.acquire();
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
          await this.execution_sem.acquire();

          this.execute(toExecute).finally(() => {
            this.execution_sem.release();
            this.add_sem.release();
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
    protected readonly queryCache: QueryCache,
    protected readonly touchTablePersistTime: number
  ) {
    super(capacity, concurrency);
  }

  protected async onCapacity(): Promise<void> {
    console.log('Too large capacity (touch)', this.queue.size);
  }

  protected async execute(tableName: string): Promise<void> {
    const key = this.queryCache.getKey('SQL_PRE_AGGREGATIONS_TABLES_TOUCH', tableName);
    console.log('touch', key);
    await this.queryCache.getCacheDriver().set(key, new Date().getTime(), this.touchTablePersistTime);
  }
}

export class TableUsedMemoryQueue extends AbstractSetMemoryQueue {
  public constructor(
    capacity: number,
    concurrency: number,
    protected readonly queryCache: QueryCache,
    protected readonly touchTablePersistTime: number
  ) {
    super(capacity, concurrency);
  }

  protected async onCapacity(): Promise<void> {
    console.log('Too large capacity (used)', this.queue.size);
  }

  protected async execute(tableName: string): Promise<void> {
    const key = this.queryCache.getKey('SQL_PRE_AGGREGATIONS_TABLES_USED', tableName);
    console.log('used', key);
    await this.queryCache.getCacheDriver().set(key, true, this.touchTablePersistTime);
  }
}
