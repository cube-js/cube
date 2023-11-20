import { QueryCache } from './QueryCache';

export abstract class AbstractSetMemoryQueue {
  protected readonly queue: Set<string> = new Set();

  public constructor(
    protected readonly capacity: number,
    protected readonly concurrency: number,
  ) {

  }

  protected execution: boolean = false;

  public addToQueue(item: string) {
    this.queue.add(item);

    if (this.queue.size > 100) {
      console.log('Too large capacity', this.queue.size);
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

          if (toExecute.length > this.concurrency) {
            break;
          }
        }

        console.log('toExecute', toExecute.length, {
          toExecute
        });

        await Promise.all(toExecute.map(async (item) => this.execute(item)));
        toExecute = [];
      } while (toExecute.length > 0);
    } finally {
      this.execution = false;
    }
  }

  abstract execute(item: string): Promise<void>;
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

  public async execute(tableName: string): Promise<void> {
    const key = this.queryCache.getKey('SQL_PRE_AGGREGATIONS_TABLES_TOUCH', tableName);
    console.log('touch', key);
    await this.queryCache.getCacheDriver().set(key, new Date().getTime(), this.touchTablePersistTime);
  }
}
