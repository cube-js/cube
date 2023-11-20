import { QueryCache } from './QueryCache';

export abstract class AbstractSetMemoryQueue {
  protected readonly queue: Set<string> = new Set();

  public constructor(
    protected readonly capacity: number,
    protected readonly concurrency: number,
  ) {

  }

  protected execution: boolean = false;

  public async addToQueue(item: string) {
    this.queue.add(item);

    if (this.queue.size > 100) {
      await this.onCapacity();
    }

    this.run().catch(e => console.log(e));
  }

  public async run(): Promise<void> {
    if (this.execution) {
      return;
    }

    this.execution = true;

    try {
      let toExecute: string[] = [];

      do {
        for (const item of this.queue) {
          toExecute.push(item);
          this.queue.delete(item);

          if (toExecute.length >= this.concurrency) {
            break;
          }
        }

        console.log('toExecute', toExecute.length, {
          toExecute
        });

        try {
          await Promise.all(toExecute.map(async (item) => this.execute(item)));
        } catch (e) {
          console.log(e);
        } finally {
          toExecute = [];
        }
      } while (this.queue.size > 0);
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
