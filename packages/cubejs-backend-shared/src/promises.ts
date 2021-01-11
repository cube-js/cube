export interface CancelablePromise<T> extends Promise<T> {
  cancel: (waitExecution?: boolean) => Promise<any>;
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
        await queued.cancel(false);
      }
    }
  }

  public defer(fn: () => Promise<void>|void): void {
    this.deferred.push(fn);
  }

  public async with(fn: CancelablePromise<void>) {
    this.withQueue.push(fn);

    return fn;
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
  promise.cancel = async (waitExecution: boolean = true) => {
    const locks: Promise<any>[] = [
      token.cancel(),
    ];

    if (waitExecution) {
      locks.push(promise);
    }

    return Promise.all(locks);
  };

  return promise;
}

export interface CancelableInterval {
  cancel: (waitExecution?: boolean) => Promise<void>,
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
      if (execution) {
        process.emitWarning(
          'Execution of previous interval was not finished, new execution will be skipped',
          'UnexpectedBehaviour'
        );

        return;
      }

      execution = createCancelablePromise(fn);

      await execution;

      execution = null;
    },
    interval,
  );

  return {
    cancel: async (waitExecution: boolean = true) => {
      clearInterval(timeout);

      if (execution) {
        await execution.cancel(waitExecution);
      }
    }
  };
}

interface RetryWithTimeoutOptions {
  timeout: number,
  intervalPause: (iteration: number) => number,
}

export const withTimeout = <T>(
  fn: CancelablePromise<T>,
  timeout: number,
): Promise<T> => {
  let timer: NodeJS.Timeout|null = null;

  return Promise.race<any>([
    fn,
    new Promise((resolve, reject) => {
      timer = setTimeout(async () => {
        await fn.cancel(false);

        reject(new Error(`Timeout reached after ${timeout}ms`));
      }, timeout);

      fn.then(resolve).catch(reject);
    })
  ]).then((v) => {
    if (timer) {
      clearTimeout(timer);
    }

    return v;
  }, (err) => {
    if (timer) {
      clearTimeout(timer);
    }

    throw err;
  });
};

export const retryWithTimeout = <T>(
  fn: (token: CancelToken) => Promise<T>,
  { timeout, intervalPause }: RetryWithTimeoutOptions,
) => withTimeout(
    createCancelablePromise<T|null>(async (token) => {
      let i = 0;

      while (!token.isCanceled()) {
        i++;

        const result = await fn(token);
        if (result) {
          return result;
        }

        await token.with(pausePromise(intervalPause(i)));
      }

      return null;
    }),
    timeout
  );
