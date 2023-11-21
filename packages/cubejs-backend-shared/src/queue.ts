import { Semaphore } from './semaphore';

/**
 * Special in-memory queue which handles execution in background with specify concurrency limit
 * It has a capacity restriction, which block adding new items to queue when the queue is full
 */
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
      this.onCapacity();
    }

    this.run().catch(e => console.error(e));
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

  protected abstract onCapacity(): void;

  protected abstract execute(item: string): Promise<void>;
}
