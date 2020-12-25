export interface CancelablePromise<T> extends Promise<T> {
  cancel: () => Promise<any>;
}

export function pausePromise(ms: number): CancelablePromise<void> {
  let cancel: Function = () => {
    //
  };

  const promise: any = new Promise((resolve) => {
    cancel = resolve;

    setTimeout(resolve, ms);
  });
  promise.cancel = cancel;

  return promise;
}

class CancelToken {
  protected readonly deferred: (() => Promise<void>|void)[] = [];

  protected readonly withQueue: (CancelablePromise<void>)[] = [];

  protected canceled = false;

  public async cancel(): Promise<void> {
    if (this.canceled) {
      throw new Error('CancelToken was already canceled');
    }

    this.canceled = true;

    if (this.deferred.length) {
      await Promise.all(this.deferred.map(async (queued) => queued()));
    }

    if (this.withQueue.length) {
      // eslint-disable-next-line no-restricted-syntax
      for (const queued of this.withQueue) {
        await queued.cancel();
      }
    }
  }

  public defer(fn: () => Promise<void>|void): void {
    this.deferred.push(fn);
  }

  public async with(fn: CancelablePromise<void>) {
    this.withQueue.push(fn);
  }

  public isCanceled() {
    return this.canceled;
  }
}

export function createCancelablePromise<T>(
  fn: (cancel: CancelToken) => Promise<T>,
): CancelablePromise<T> {
  const token = new CancelToken();

  const promise: any = fn(token);
  promise.cancel = async () => Promise.all([
    token.cancel(),
    promise
  ]);

  return promise;
}

export interface CancelableInterval {
  cancel: () => Promise<void>,
}

/**
 * It's helps to create an interval that can be canceled with awaiting latest execution
 */
export function createCancelableInterval<T>(
  fn: (token: CancelToken) => Promise<T>,
  interval: number,
): CancelableInterval {
  let execution: CancelablePromise<T>|null = null;

  const timeout = setInterval(
    async () => {
      execution = createCancelablePromise(fn);

      await execution;

      execution = null;
    },
    interval,
  );

  return {
    cancel: async () => {
      clearInterval(timeout);

      if (execution) {
        await execution.cancel();

        await execution;
      }
    }
  };
}
