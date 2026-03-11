import crypto from 'crypto';
import { createCancelablePromise, pausePromise } from '@cubejs-backend/shared';

import { QueryCache, QueryCacheOptions } from '../../src';

export type QueryCacheTestOptions = QueryCacheOptions & {
  beforeAll?: () => Promise<void>,
  afterAll?: () => Promise<void>,
};

export const QueryCacheTest = (name: string, options: QueryCacheTestOptions) => {
  describe(`QueryQueue${name}`, () => {
    const cache = new QueryCache(
      crypto.randomBytes(16).toString('hex'),
      jest.fn(() => {
        throw new Error('It`s not implemented mock...');
      }),
      jest.fn(),
      options,
    );

    beforeAll(async () => {
      if (options?.beforeAll) {
        await options?.beforeAll();
      }
    });

    afterAll(async () => {
      await cache.cleanup();

      if (options?.afterAll) {
        await options?.afterAll();
      }
    });

    it('withLock', async () => {
      const RANDOM_KEY_CACHE = crypto.randomBytes(16).toString('hex');

      const testLock = async () => {
        let started = 0;
        let finished = 0;

        const doLock = (sleep: number) => cache.withLock(
          RANDOM_KEY_CACHE,
          60 * 10,
          async () => {
            started++;

            await pausePromise(sleep);

            finished++;
          },
        );

        const locks: Promise<boolean>[] = [
          doLock(1000)
        ];

        await pausePromise(100);

        locks.push(doLock(1000));
        locks.push(doLock(1000));

        const results = await Promise.all(locks);
        expect(results[0]).toEqual(true);
        expect(results[1]).toEqual(false);
        expect(results[2]).toEqual(false);

        expect(started).toEqual(1);
        expect(finished).toEqual(1);
      };

      await testLock();

      await pausePromise(500);

      await testLock();
    });

    it('withLock + cancel (test free of lock + cancel inheritance)', async () => {
      const RANDOM_KEY_CACHE = crypto.randomBytes(16).toString('hex');

      const lockPromise = cache.withLock(
        RANDOM_KEY_CACHE,
        60 * 10,
        () => createCancelablePromise(async (tkn) => {
          await tkn.with(
            // This timeout is useful to test that withLock.cancel use callback as tkn.with
            // If doesn't use it, test will fail with timeout
            pausePromise(60 * 60 * 1000)
          );
        }),
      );

      await lockPromise.cancel(true);
      await lockPromise;

      let callbackWasExecuted = false;

      // withLock return boolean, where true success execution & lock
      const statusOfResolve = await cache.withLock(
        RANDOM_KEY_CACHE,
        60 * 10,
        async () => {
          callbackWasExecuted = true;
        },
      );

      expect(statusOfResolve).toEqual(true);
      expect(callbackWasExecuted).toEqual(true);
    });

    describe('cacheQueryResult renewal logic', () => {
      const renewalKeyA = QueryCache.queryCacheKey({ query: 'key-a', values: [] });
      const renewalKeyOld = QueryCache.queryCacheKey({ query: 'key-old', values: [] });
      const renewalKeyNew = QueryCache.queryCacheKey({ query: 'key-new', values: [] });

      // Helper to seed a cache entry directly
      const seedCache = async (cacheKey, entry) => {
        const redisKey = cache.queryRedisKey(cacheKey);
        await cache.getCacheDriver().set(redisKey, entry, 3600);
      };

      const callCacheQueryResult = async (
        cacheKey,
        cacheEntry,
        opts: {
          renewalThreshold?: number;
          renewalKey?;
          waitForRenew?: boolean;
          requestId?: string;
          renewCycle?: boolean;
        }
      ) => {
        // cacheQueryResult hashes options.renewalKey via queryRedisKey(),
        // and fetchNew() stores that hash in the entry. Replicate that for seeding.
        const seededEntry = {
          ...cacheEntry,
          renewalKey: cacheEntry.renewalKey
            ? cache.queryRedisKey(cacheEntry.renewalKey)
            : cacheEntry.renewalKey,
        };
        await seedCache(cacheKey, seededEntry);

        const fetchNewCalled = { value: false, blocked: false };
        const spy = jest.spyOn(cache, 'queryWithRetryAndRelease').mockImplementation(async () => {
          fetchNewCalled.value = true;
          return 'new-result';
        });

        try {
          const result = await cache.cacheQueryResult(
            'SELECT 1',
            [],
            cacheKey,
            3600,
            {
              renewalThreshold: opts.renewalThreshold ?? 600,
              renewalKey: opts.renewalKey,
              waitForRenew: opts.waitForRenew ?? false,
              requestId: opts.requestId,
              dataSource: 'default',
              renewCycle: opts.renewCycle,
            }
          );

          fetchNewCalled.blocked = result === 'new-result';

          return { result, fetchNewCalled: fetchNewCalled.value, blocked: fetchNewCalled.blocked };
        } finally {
          spy.mockRestore();
        }
      };

      it('expired + waitForRenew: blocks on fetchNew', async () => {
        const cacheKey = QueryCache.queryCacheKey({ query: 'expired-wait', values: [] });
        const entry = {
          time: Date.now() - 700 * 1000,
          result: 'cached-data',
          renewalKey: renewalKeyA,
        };

        const { result, blocked } = await callCacheQueryResult(cacheKey, entry, {
          renewalThreshold: 600,
          renewalKey: renewalKeyA,
          waitForRenew: true,
          requestId: 'req-1',
        });

        expect(blocked).toBe(true);
        expect(result).toBe('new-result');
      });

      it('expired + no waitForRenew: returns cached, background refresh', async () => {
        const cacheKey = QueryCache.queryCacheKey({ query: 'expired-no-wait', values: [] });
        const entry = {
          time: Date.now() - 700 * 1000,
          result: 'cached-data',
          renewalKey: renewalKeyA,
        };

        const { result, fetchNewCalled, blocked } = await callCacheQueryResult(cacheKey, entry, {
          renewalThreshold: 600,
          renewalKey: renewalKeyA,
          waitForRenew: false,
          requestId: 'req-2',
        });

        expect(result).toBe('cached-data');
        expect(fetchNewCalled).toBe(true);
        expect(blocked).toBe(false);
      });

      it('key mismatch + not expired + user request: returns cached, background refresh', async () => {
        const cacheKey = QueryCache.queryCacheKey({ query: 'key-mismatch-user', values: [] });
        const entry = {
          time: Date.now() - 100 * 1000,
          result: 'cached-data',
          renewalKey: renewalKeyOld,
        };

        const { result, fetchNewCalled, blocked } = await callCacheQueryResult(cacheKey, entry, {
          renewalThreshold: 600,
          renewalKey: renewalKeyNew,
          waitForRenew: true,
          renewCycle: false,
          requestId: 'req-3',
        });

        expect(result).toBe('cached-data');
        expect(fetchNewCalled).toBe(true);
        expect(blocked).toBe(false);
      });

      it('key mismatch + not expired + renew cycle: blocks on fetchNew', async () => {
        const cacheKey = QueryCache.queryCacheKey({ query: 'key-mismatch-renew', values: [] });
        const entry = {
          time: Date.now() - 100 * 1000,
          result: 'cached-data',
          renewalKey: renewalKeyOld,
        };

        const { result, blocked } = await callCacheQueryResult(cacheKey, entry, {
          renewalThreshold: 600,
          renewalKey: renewalKeyNew,
          waitForRenew: true,
          renewCycle: true,
          requestId: 'req-4',
        });

        expect(blocked).toBe(true);
        expect(result).toBe('new-result');
      });

      it('same request + expired: returns cached, background refresh', async () => {
        const cacheKey = QueryCache.queryCacheKey({ query: 'same-req-expired', values: [] });
        const entry = {
          time: Date.now() - 700 * 1000,
          result: 'cached-data',
          renewalKey: renewalKeyOld,
          requestId: 'req-5',
        };

        const { result, fetchNewCalled, blocked } = await callCacheQueryResult(cacheKey, entry, {
          renewalThreshold: 600,
          renewalKey: renewalKeyNew,
          waitForRenew: true,
          requestId: 'req-5',
        });

        expect(result).toBe('cached-data');
        expect(fetchNewCalled).toBe(true);
        expect(blocked).toBe(false);
      });

      it('same request + key mismatch only: returns cached, background refresh', async () => {
        const cacheKey = QueryCache.queryCacheKey({ query: 'same-req-key-mismatch', values: [] });
        const entry = {
          time: Date.now() - 100 * 1000,
          result: 'cached-data',
          renewalKey: renewalKeyOld,
          requestId: 'req-6',
        };

        const { result, fetchNewCalled, blocked } = await callCacheQueryResult(cacheKey, entry, {
          renewalThreshold: 600,
          renewalKey: renewalKeyNew,
          waitForRenew: true,
          requestId: 'req-6',
        });

        expect(result).toBe('cached-data');
        expect(fetchNewCalled).toBe(true);
        expect(blocked).toBe(false);
      });

      it('key matches + not expired: returns cached, no fetchNew', async () => {
        const cacheKey = QueryCache.queryCacheKey({ query: 'key-match-fresh', values: [] });
        const entry = {
          time: Date.now() - 100 * 1000,
          result: 'cached-data',
          renewalKey: renewalKeyA,
        };

        const { result, fetchNewCalled, blocked } = await callCacheQueryResult(cacheKey, entry, {
          renewalThreshold: 600,
          renewalKey: renewalKeyA,
          waitForRenew: true,
          requestId: 'req-7',
        });

        expect(result).toBe('cached-data');
        expect(fetchNewCalled).toBe(false);
        expect(blocked).toBe(false);
      });
    });

    it('queryCacheKey format', () => {
      const key1 = QueryCache.queryCacheKey({
        query: 'select data',
        values: ['value'],
        preAggregations: [],
        invalidate: [],
        persistent: true,
      });
      expect(key1[0]).toEqual('select data');
      expect(key1[1]).toEqual(['value']);
      expect(key1[2]).toEqual([]);
      expect(key1[3]).toEqual([]);
      // @ts-ignore
      expect(key1.persistent).toEqual(true);

      const key2 = QueryCache.queryCacheKey({
        query: 'select data',
        values: ['value'],
        preAggregations: [],
        invalidate: [],
        persistent: false,
      });
      expect(key2[0]).toEqual('select data');
      expect(key2[1]).toEqual(['value']);
      expect(key2[2]).toEqual([]);
      expect(key2[3]).toEqual([]);
      // @ts-ignore
      expect(key2.persistent).toEqual(false);

      const key3 = QueryCache.queryCacheKey({
        query: 'select data',
        values: ['value'],
        persistent: true,
      });
      expect(key3[0]).toEqual('select data');
      expect(key3[1]).toEqual(['value']);
      expect(key3[2]).toEqual([]);
      expect(key3[3]).toBeUndefined();
      // @ts-ignore
      expect(key3.persistent).toEqual(true);

      const key4 = QueryCache.queryCacheKey({
        query: 'select data',
        values: ['value'],
        persistent: false,
      });
      expect(key4[0]).toEqual('select data');
      expect(key4[1]).toEqual(['value']);
      expect(key4[2]).toEqual([]);
      expect(key4[3]).toBeUndefined();
      // @ts-ignore
      expect(key4.persistent).toEqual(false);
    });
  });
};
