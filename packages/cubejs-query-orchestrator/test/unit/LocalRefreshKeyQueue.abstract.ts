import crypto from 'crypto';
import { pausePromise } from '@cubejs-backend/shared';
import { QueryCache, QueryCacheOptions, QueryWithParams } from '../../src';
import { evaluateLocalRefreshKey } from '../../src/orchestrator/utils';

const deferred = () => {
  let resolve: () => void;
  const promise = new Promise<void>(done => { resolve = done; });
  return { promise, resolve: () => resolve() };
};

// Runs with both the memory and CubeStore cache/queue drivers through QueryCacheTest.
export const localRefreshKeyQueueTests = (options: QueryCacheOptions) => {
  describe('cached local refresh key queue execution', () => {
    const caches: QueryCache[] = [];
    const descriptor = { interval: 600, utcOffset: 0, dayOffset: 0 };
    const sql = 'SELECT FLOOR(UNIX_TIMESTAMP() / 600) as refresh_key';
    const make = (prefix = crypto.randomBytes(16).toString('hex'), logger = jest.fn(), localRefreshKey = true) => {
      const factory = jest.fn(async () => {
        if (localRefreshKey) throw new Error('local refresh keys must not create a database client');
        // SQL stand-in for the one-second key in the expiry comparison below.
        return { query: async () => [{ refresh_key: String(Math.floor(Date.now() / 1000)) }] } as any;
      });
      const cache = new QueryCache(prefix, factory, logger, {
        ...options,
        localRefreshKey,
        refreshKeyRenewalThreshold: 86400,
        queueOptions: async () => ({ concurrency: 1 }),
        externalQueueOptions: { concurrency: 1 },
        externalDriverFactory: factory,
      });
      caches.push(cache);
      return { cache, factory };
    };
    afterEach(async () => {
      jest.restoreAllMocks();
      await Promise.all(caches.splice(0).map(cache => cache.cleanup()));
    });

    test.each([false, true])('shares a cached local result without a database client (external=%s)', async external => {
      const prefix = crypto.randomBytes(16).toString('hex');
      const first = make(prefix);
      const second = make(prefix);
      const q: QueryWithParams = [sql, [], { external, localRefreshKey: descriptor }];
      const before = evaluateLocalRefreshKey(descriptor);
      const value = await first.cache.cacheRefreshKeyResult(q, 3600, { dataSource: 'default', waitForRenew: true });
      expect([before[0].refresh_key, evaluateLocalRefreshKey(descriptor)[0].refresh_key]).toContain(value[0].refresh_key);
      expect(await second.cache.cacheRefreshKeyResult(q, 86400, { dataSource: 'default', waitForRenew: true })).toEqual(value);
      const key = first.cache.refreshKeyCacheKey(q, 'default');
      expect(await second.cache.getCacheDriver().get(key)).toMatchObject({ result: value, renewalKey: key });
      expect(first.factory).not.toHaveBeenCalled();
      expect(second.factory).not.toHaveBeenCalled();
    });

    test('matches SQL after shared cache expiry, including retained queue results', async () => {
      const observations = [];
      const now = Date.now();
      const clock = jest.spyOn(Date, 'now');
      for (const localRefreshKey of [false, true]) {
        const prefix = crypto.randomBytes(16).toString('hex');
        const first = make(prefix, jest.fn(), localRefreshKey);
        const second = make(prefix, jest.fn(), localRefreshKey);
        const q: QueryWithParams = ['SELECT FLOOR(UNIX_TIMESTAMP()) as refresh_key', [], {
          localRefreshKey: { ...descriptor, interval: 1 },
        }];
        clock.mockReturnValue(now);
        const before = await first.cache.cacheRefreshKeyResult(q, 1, { dataSource: 'default', waitForRenew: true });
        await pausePromise(1200);
        expect(await second.cache.getCacheDriver().get(first.cache.refreshKeyCacheKey(q, 'default'))).toBeFalsy();
        clock.mockReturnValue(now + 2000);
        const enqueue = jest.spyOn(second.cache, 'queryWithRetryAndRelease');
        const after = await second.cache.cacheRefreshKeyResult(q, 1, { dataSource: 'default', waitForRenew: true });
        expect(enqueue).toHaveBeenCalledTimes(1);
        observations.push({ before, after });
        if (localRefreshKey) {
          expect(first.factory).not.toHaveBeenCalled();
          expect(second.factory).not.toHaveBeenCalled();
        }
      }
      // CubeStore can return a retained queue result even after the result cache entry expired.
      // Preserve this existing SQL behavior rather than imposing stronger freshness here.
      expect(observations[1]).toEqual(observations[0]);
    });

    test.each([false, true])('deduplicates concurrent misses across instances and evaluates at execution (external=%s)', async external => {
      const prefix = crypto.randomBytes(16).toString('hex');
      const enqueued = deferred();
      const first = make(prefix);
      const second = make(prefix, jest.fn((message) => {
        if (message === 'Waiting for query') enqueued.resolve();
      }));
      const firstQueue = external ? first.cache.getExternalQueue() : await first.cache.getQueue('default');
      const secondQueue = external ? second.cache.getExternalQueue() : await second.cache.getQueue('default');
      const entered = deferred();
      const release = deferred();
      const hold = queue => {
        const handler = queue['queryHandlers'].query;
        return jest.spyOn(queue['queryHandlers'], 'query').mockImplementation(async (...args: any[]) => {
          entered.resolve();
          await release.promise;
          return handler(...args);
        });
      };
      const firstHandler = hold(firstQueue);
      const secondHandler = hold(secondQueue);
      const q: QueryWithParams = [sql, [], { external, localRefreshKey: descriptor }];
      const now = Date.now();
      const nextBoundary = Math.ceil(now / 600000) * 600000;
      const clock = jest.spyOn(Date, 'now').mockReturnValue(nextBoundary - 1);
      const firstResult = first.cache.cacheRefreshKeyResult(q, 3600, { dataSource: 'default', waitForRenew: true });
      let secondResult: ReturnType<QueryCache['cacheRefreshKeyResult']> | undefined;
      try {
        await entered.promise;
        secondResult = second.cache.cacheRefreshKeyResult(q, 86400, { dataSource: 'default', waitForRenew: true });
        await enqueued.promise;
        clock.mockReturnValue(nextBoundary);
        release.resolve();
        const values = await Promise.all([firstResult, secondResult]);
        expect(values).toEqual([evaluateLocalRefreshKey(descriptor, nextBoundary), evaluateLocalRefreshKey(descriptor, nextBoundary)]);
        expect(firstHandler.mock.calls.length + secondHandler.mock.calls.length).toBe(1);
        expect(first.factory).not.toHaveBeenCalled();
        expect(second.factory).not.toHaveBeenCalled();
      } finally {
        release.resolve();
        await Promise.allSettled([firstResult, secondResult]);
        clock.mockRestore();
      }
    });
  });
};
