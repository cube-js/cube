export class Semaphore {
  protected readonly queue: (() => void)[] = [];

  protected permits: number;

  public constructor(
    protected readonly num: number
  ) {
    this.permits = num;
  }

  public release(): void {
    this.permits++;

    if (this.permits > 0 && this.queue.length > 0) {
      this.runQueue();
    }
  }

  protected runQueue() {
    const nexResolver = this.queue.shift();
    if (nexResolver) {
      this.permits--;

      nexResolver();
      return true;
    }

    return false;
  }

  public acquire(): Promise<void> {
    if (this.permits > 0) {
      this.permits--;

      return Promise.resolve();
    }

    return new Promise<void>(
      (resolve) => {
        this.queue.push(resolve);
      }
    );
  }

  public async execute<T>(func: () => T | PromiseLike<T>): Promise<T> {
    await this.acquire();

    try {
      return await func();
    } finally {
      this.release();
    }
  }
}

export class Mutex extends Semaphore {
  public constructor() {
    super(1);
  }
}
