/* eslint-disable arrow-body-style,no-restricted-syntax */
import crypto from 'crypto';
import { LRUCache } from 'lru-cache';

import { Optional } from './type-helpers';

export type PromiseLock = {
  promise: Promise<void>,
  resolve: () => void,
};

export function createPromiseLock(): PromiseLock {
  let resolve: any = null;

  return {
    promise: new Promise<void>((resolver) => {
      resolve = resolver;
    }),
    resolve: () => {
      resolve();
    }
  };
}

export type CancelablePromiseCancel = (waitExecution?: boolean) => Promise<any>;

export interface CancelablePromise<T> extends Promise<T> {
  cancel: CancelablePromiseCancel;
}

export type MaybeCancelablePromise<T> = Optional<CancelablePromise<T>, 'cancel'>;

export function pausePromise(ms: number): CancelablePromise<void> {
  let cancel: Function = () => {
    //
  };

  const promise: any = new Promise<void>((resolve) => {
    const timeout = setTimeout(resolve, ms);

    cancel = () => {
      clearTimeout(timeout);

      resolve();
    };
  });
  promise.cancel = cancel;

  return promise;
}

class CancelToken {
  protected readonly deferred: (() => Promise<void> | void)[] = [];

  protected readonly withQueue: CancelablePromiseCancel[] = [];

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
      await Promise.all(
        this.withQueue.map((cb) => cb())
      );
    }
  }

  public defer(fn: () => Promise<void> | void): void {
    this.deferred.push(fn);
  }

  public async with<T = any>(fn: MaybeCancelablePromise<T>): Promise<T> {
    if (fn.cancel) {
      this.withQueue.push(fn.cancel);
    }

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

export interface CancelableIntervalOptions {
  interval: number,
  onDuplicatedStateResolved?: (intervalId: number, elapsedTime: number) => any,
  onDuplicatedExecution?: (intervalId: number) => any,
}

/**
 * It helps to create an interval that can be canceled with awaiting latest execution
 */
export function createCancelableInterval<T>(
  fn: (token: CancelToken) => Promise<T>,
  options: CancelableIntervalOptions,
): CancelableInterval {
  let execution: CancelablePromise<T> | null = null;
  let startTime: number | null = null;
  let intervalId: number = 0;
  let duplicatedExecutionTracked: boolean = false;

  const timerId = setInterval(
    async () => {
      if (execution) {
        if (options.onDuplicatedExecution) {
          duplicatedExecutionTracked = true;
          options.onDuplicatedExecution(intervalId);
        }

        return;
      }

      try {
        intervalId++;

        if (intervalId >= Number.MAX_SAFE_INTEGER) {
          intervalId = 0;
        }

        startTime = Date.now();
        execution = createCancelablePromise(fn);

        await execution;
      } finally {
        execution = null;

        if (duplicatedExecutionTracked && options.onDuplicatedStateResolved) {
          options.onDuplicatedStateResolved(intervalId, Date.now() - <number>startTime);
        }

        duplicatedExecutionTracked = false;
      }
    },
    options.interval,
  );

  return {
    cancel: async (waitExecution: boolean = true) => {
      clearInterval(timerId);

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

export const withTimeout = (
  fn: (...args: any[]) => void,
  timeout: number,
): CancelablePromise<any> => {
  // eslint-disable-next-line @typescript-eslint/no-empty-function
  let cancel: Function = () => {};

  const promise: any = new Promise<void>((resolve) => {
    const timer = setTimeout(() => {
      fn();

      resolve();
    }, timeout);

    cancel = () => {
      clearTimeout(timer);

      resolve();
    };
  });
  promise.cancel = cancel;

  return promise;
};

export const withTimeoutRace = <T>(
  fn: CancelablePromise<T>,
  timeout: number,
): Promise<T> => {
  let timer: NodeJS.Timeout | null = null;

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
) => withTimeoutRace(
    createCancelablePromise<T | null>(async (token) => {
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

export type AsyncDebounceOptions = {
  max?: number;
  ttl?: number;
};

export const asyncDebounceFn = <Ret, Arguments>(
  fn: (...args: Arguments[]) => Promise<Ret>,
  options: AsyncDebounceOptions = {}
) => {
  const { max = 100, ttl = 60 * 1000 } = options;

  const cache = new LRUCache<string, Promise<Ret>>({
    max,
    ttl,
  });

  return async (...args: Arguments[]) => {
    const key = crypto.createHash('md5')
      .update(args.map((v) => JSON.stringify(v)).join(','))
      .digest('hex');

    const existing = cache.get(key);
    if (existing) {
      return existing;
    }

    try {
      const promise = fn(...args);
      cache.set(key, promise);
      return await promise;
    } finally {
      cache.delete(key);
    }
  };
};

export type MemoizeOptions<Ret, Arguments> = {
  extractKey: (...args: Arguments[]) => string,
  extractCacheLifetime: (result: Ret) => number,
};

type MemoizeBucket<T> = {
  item: T,
  lifetime: number,
};

export const asyncMemoize = <Ret, Arguments>(
  fn: (...args: Arguments[]) => Promise<Ret>,
  options: MemoizeOptions<Ret, Arguments>
) => {
  const cache = new Map<string, MemoizeBucket<Ret>>();

  const debouncedFn = asyncDebounceFn(fn);

  const call = async (...args: Arguments[]) => {
    const key = options.extractKey(...args);

    if (cache.has(key)) {
      const bucket = <MemoizeBucket<Ret>>cache.get(key);
      if (bucket.lifetime >= Date.now()) {
        return bucket.item;
      } else {
        cache.delete(key);
      }
    }

    const item = await debouncedFn(...args);
    cache.set(key, {
      lifetime: Date.now() + options.extractCacheLifetime(item),
      item,
    });

    return item;
  };

  call.force = async (...args: Arguments[]) => {
    const key = options.extractKey(...args);

    const item = await debouncedFn(...args);
    cache.set(key, {
      lifetime: Date.now() + options.extractCacheLifetime(item),
      item,
    });

    return item;
  };

  return call;
};

export type BackgroundMemoizeOptions<Ret, Arguments> = {
  extractKey: (...args: Arguments[]) => string,
  extractCacheLifetime: (result: Ret) => number,
  backgroundRefreshInterval: number,
  onBackgroundException: (err: Error) => void,
};

type BackgroundMemoizeBucket<T, A> = {
  item: T,
  args: A[],
  lifetime: number,
};

export const decorateWithCancel = <T, C = () => void>(fn: Promise<T>, cancel: C): CancelablePromise<T> => {
  (<any>fn).cancel = cancel;

  return <any>fn;
};

export const asyncMemoizeBackground = <Ret, Arguments>(
  fn: (...args: Arguments[]) => Promise<Ret>,
  options: BackgroundMemoizeOptions<Ret, Arguments>
) => {
  const cache = new Map<string, BackgroundMemoizeBucket<Ret, Arguments>>();

  const debouncedFn = asyncDebounceFn(fn);

  const refreshBucket = async (bucket: BackgroundMemoizeBucket<Ret, Arguments>) => {
    try {
      const item = await debouncedFn(...bucket.args);

      bucket.item = item;
      bucket.lifetime = Date.now() + options.extractCacheLifetime(item);
    } catch (e: any) {
      options.onBackgroundException(e);
    }
  };

  const refreshInterval = createCancelableInterval(async () => {
    const refreshBatch: Promise<any>[] = [];
    const now = Date.now();

    for (const bucket of cache.values()) {
      if (bucket.lifetime < now) {
        refreshBatch.push(refreshBucket(bucket));
      }
    }

    return Promise.all(refreshBatch);
  }, {
    interval: options.backgroundRefreshInterval,
  });

  const call = async (...args: Arguments[]) => {
    const key = options.extractKey(...args);

    if (cache.has(key)) {
      // If cache exists, only background timer or force can update it.
      return (<MemoizeBucket<Ret>>cache.get(key)).item;
    }

    const item = await debouncedFn(...args);
    cache.set(key, {
      lifetime: Date.now() + options.extractCacheLifetime(item),
      args,
      item,
    });

    return item;
  };

  call.force = async (...args: Arguments[]) => {
    const key = options.extractKey(...args);

    const item = await debouncedFn(...args);
    cache.set(key, {
      lifetime: Date.now() + options.extractCacheLifetime(item),
      args,
      item,
    });

    return item;
  };

  call.release = refreshInterval.cancel;

  return call;
};

export type RetryOptions = {
  times: number,
};

/**
 * High order function that do retry when async function throw an exception
 */
export const asyncRetry = async <Ret>(
  fn: () => Promise<Ret>,
  options: RetryOptions
) => {
  if (options.times <= 0) {
    throw new Error('Option times in asyncRetry, must be a positive integer');
  }

  let latestException: unknown = null;

  for (let i = 0; i < options.times; i++) {
    try {
      return await fn();
    } catch (e) {
      latestException = e;
    }
  }

  throw latestException;
};
