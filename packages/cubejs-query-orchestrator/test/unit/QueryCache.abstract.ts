import crypto from 'crypto';
import { CacheMode, createCancelablePromise, pausePromise } from '@cubejs-backend/shared';
import { QueuePriority } from '@cubejs-backend/base-driver';

import { CacheKey, CacheKeyItem, ContinueWaitError, QueryCache, QueryCacheOptions } from '../../src';

export type QueryCacheTestOptions = QueryCacheOptions & {
  beforeAll?: () => Promise<void>,
  afterAll?: () => Promise<void>,
};

class QueryCacheOpened extends QueryCache {
  public readonly logger = jest.fn(super.logger);
}

export const QueryCacheTest = (name: string, options: QueryCacheTestOptions) => {
  describe(`QueryQueue${name}`, () => {
    const cache = new QueryCacheOpened(
      crypto.randomBytes(16).toString('hex'),
      () => {
        throw new Error('driverFactory is not implemented, mock should be used...');
      },
      jest.fn(() => {
        throw new Error('logger is not implemented, mock should be used...');
      }),
      options,
    );

    beforeEach(() => {
      cache.logger.mockClear();
    });

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

      const seedCache = async (cacheKey: CacheKey, entry: CacheKeyItem) => {
        const redisKey = cache.queryCacheKey(cacheKey);
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
        // cacheQueryResult hashes options.renewalKey via queryCacheKey(),
        // and fetchNew() stores that hash in the entry. Replicate that for seeding.
        const seededEntry = {
          ...cacheEntry,
          renewalKey: cacheEntry.renewalKey
            ? cache.queryCacheKey(cacheEntry.renewalKey)
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
        expect(cache.logger.mock.calls.map(c => c[0])).toContain('Waiting for renew');
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
        expect(cache.logger.mock.calls.map(c => c[0])).toContain('Renewing existing key');
      });

      it('key mismatch + not expired + waitForRenew: blocks on fetchNew', async () => {
        const cacheKey = QueryCache.queryCacheKey({ query: 'key-mismatch-user', values: [] });
        const entry = {
          time: Date.now() - 100 * 1000,
          result: 'cached-data',
          renewalKey: renewalKeyOld,
        };

        const { result, blocked } = await callCacheQueryResult(cacheKey, entry, {
          renewalThreshold: 600,
          renewalKey: renewalKeyNew,
          waitForRenew: true,
          renewCycle: false,
          requestId: 'req-3',
        });

        expect(blocked).toBe(true);
        expect(result).toBe('new-result');
        expect(cache.logger.mock.calls.map(c => c[0])).toContain('Waiting for renew');
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
        expect(cache.logger.mock.calls.map(c => c[0])).toContain('Waiting for renew');
      });

      it('same request + expired: returns cached, background refresh', async () => {
        const cacheKey = QueryCache.queryCacheKey({ query: 'same-req-expired', values: [] });
        const entry = {
          time: Date.now() - 700 * 1000,
          result: 'cached-data',
          renewalKey: renewalKeyOld,
          requestId: 'abc-123-span-1',
        };

        const { result, fetchNewCalled, blocked } = await callCacheQueryResult(cacheKey, entry, {
          renewalThreshold: 600,
          renewalKey: renewalKeyNew,
          waitForRenew: true,
          requestId: 'abc-123-span-2',
        });

        expect(result).toBe('cached-data');
        expect(fetchNewCalled).toBe(true);
        expect(blocked).toBe(false);
        expect(cache.logger.mock.calls.map(c => c[0])).toContain('Same request cache hit (background refresh)');
      });

      it('same request + key mismatch only: returns cached, background refresh', async () => {
        const cacheKey = QueryCache.queryCacheKey({ query: 'same-req-key-mismatch', values: [] });
        const entry = {
          time: Date.now() - 100 * 1000,
          result: 'cached-data',
          renewalKey: renewalKeyOld,
          requestId: 'conn-456-sub-789-span-aaa',
        };

        const { result, fetchNewCalled, blocked } = await callCacheQueryResult(cacheKey, entry, {
          renewalThreshold: 600,
          renewalKey: renewalKeyNew,
          waitForRenew: true,
          requestId: 'conn-456-sub-789-span-bbb',
        });

        expect(result).toBe('cached-data');
        expect(fetchNewCalled).toBe(true);
        expect(blocked).toBe(false);
        expect(cache.logger.mock.calls.map(c => c[0])).toContain('Same request cache hit (background refresh)');
      });

      it('same request + renewCycle + key mismatch: must block on fetchNew (not return stale cache)', async () => {
        const cacheKey = QueryCache.queryCacheKey({ query: 'same-req-renew-cycle', values: [] });
        const entry = {
          time: Date.now() - 100 * 1000,
          result: 'stale-data',
          renewalKey: renewalKeyOld,
          requestId: 'req-cycle-span-1',
        };

        const { result, blocked } = await callCacheQueryResult(cacheKey, entry, {
          renewalThreshold: 600,
          renewalKey: renewalKeyNew,
          waitForRenew: true,
          renewCycle: true,
          requestId: 'req-cycle-span-2',
        });

        // renewCycle must always fetch fresh data even when requestId matches
        expect(blocked).toBe(true);
        expect(result).toBe('new-result');
        expect(cache.logger.mock.calls.map(c => c[0])).toContain('Waiting for renew');
      });

      it('same request + renewCycle + expired: must block on fetchNew', async () => {
        const cacheKey = QueryCache.queryCacheKey({ query: 'same-req-renew-cycle-expired', values: [] });
        const entry = {
          time: Date.now() - 700 * 1000,
          result: 'stale-data',
          renewalKey: renewalKeyOld,
          requestId: 'req-exp-cycle-span-1',
        };

        const { result, blocked } = await callCacheQueryResult(cacheKey, entry, {
          renewalThreshold: 600,
          renewalKey: renewalKeyNew,
          waitForRenew: true,
          renewCycle: true,
          requestId: 'req-exp-cycle-span-2',
        });

        expect(blocked).toBe(true);
        expect(result).toBe('new-result');
        expect(cache.logger.mock.calls.map(c => c[0])).toContain('Waiting for renew');
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
        expect(cache.logger.mock.calls.map(c => c[0])).not.toContain('Waiting for renew');
        expect(cache.logger.mock.calls.map(c => c[0])).not.toContain('Renewing existing key');
      });
    });

    describe('cachedQueryResult cold cache (backgroundRenew: false)', () => {
      beforeAll(() => {
        expect(cache.options.backgroundRenew).toBe(false);
      });

      it('executes the main query only once instead of racing two fetches for the same key', async () => {
        const mainQuery = `SELECT cold-cache-main-${crypto.randomBytes(8).toString('hex')}`;
        const cacheKeyQuery = `SELECT cold-cache-refresh-key-${crypto.randomBytes(8).toString('hex')}`;

        // Mock below QueryCache and above QueryQueue so duplicate cache submissions remain observable.
        const querySpy = jest.spyOn(cache, 'queryWithRetryAndRelease').mockImplementation(async (query) => {
          if (query === mainQuery) {
            return [{ result: 'ok' }];
          }

          if (query === cacheKeyQuery) {
            return [{ refresh_key: '1' }];
          }

          throw new Error(`Unexpected query: ${JSON.stringify(query)}`);
        });
        const queryCallCount = (targetQuery: string) => querySpy.mock.calls
          .filter(([query]) => query === targetQuery).length;
        const renewQuerySpy = jest.spyOn(cache, 'renewQuery');
        const startRenewCycle = cache.startRenewCycle.bind(cache);
        let mainQueryCallsAtRenewCycleStart: number | undefined;
        const renewCycleSpy = jest.spyOn(cache, 'startRenewCycle').mockImplementation((...args) => {
          mainQueryCallsAtRenewCycleStart = queryCallCount(mainQuery);
          return startRenewCycle(...args);
        });
        let renewCyclePromise: Promise<unknown> | undefined;

        try {
          const result = await cache.cachedQueryResult(
            {
              query: mainQuery,
              values: [],
              cacheKeyQueries: [[cacheKeyQuery, []]],
              requestId: 'cold-cache-req',
              dataSource: 'default',
            },
            [],
          );

          const renewCycleCallIndex = renewQuerySpy.mock.calls.findIndex(
            ([, , , , , , renewOptions]) => renewOptions.renewCycle
          );
          if (renewCycleCallIndex !== -1) {
            renewCyclePromise = renewQuerySpy.mock.results[renewCycleCallIndex].value;
            await renewCyclePromise;
          }

          expect(renewCycleCallIndex).not.toBe(-1);
          expect(result.data).toEqual([{ result: 'ok' }]);
          expect(mainQueryCallsAtRenewCycleStart).toBe(1);
          expect(queryCallCount(cacheKeyQuery)).toBe(1);
          expect(queryCallCount(mainQuery)).toBe(1);
          expect(renewCycleSpy).toHaveBeenCalledTimes(1);
        } finally {
          await renewCyclePromise?.catch(() => undefined);
          renewCycleSpy.mockRestore();
          renewQuerySpy.mockRestore();
          querySpy.mockRestore();
        }
      });

      it.each([
        { type: 'ContinueWaitError', error: new ContinueWaitError() },
        { type: 'a generic error', error: new Error('driver failed') },
      ])('does not start a renew cycle when the foreground renewal fails with $type', async ({ error }) => {
        const renewQuerySpy = jest.spyOn(cache, 'renewQuery').mockRejectedValue(error);
        const renewCycleSpy = jest.spyOn(cache, 'startRenewCycle');

        try {
          await expect(cache.cachedQueryResult(
            {
              query: 'SELECT continue-wait-main',
              values: [],
              cacheKeyQueries: [['SELECT continue-wait-refresh-key', []]],
              requestId: 'continue-wait-req',
              dataSource: 'default',
            },
            [],
          )).rejects.toBe(error);

          expect(renewCycleSpy).not.toHaveBeenCalled();
        } finally {
          renewCycleSpy.mockRestore();
          renewQuerySpy.mockRestore();
        }
      });

      // The queue fast track only engages at `QueuePriority.Interactive`, so a request-blocked
      // query that loses its priority on the way down silently falls back to the slow path.
      it.each<{ type: string, cacheMode?: CacheMode, queuePriority?: number, expected: number }>([
        { type: 'the default', cacheMode: undefined, queuePriority: undefined, expected: QueuePriority.Interactive },
        { type: 'an explicit queuePriority', cacheMode: undefined, queuePriority: 42, expected: 42 },
        { type: 'must-revalidate', cacheMode: 'must-revalidate', queuePriority: undefined, expected: QueuePriority.Interactive },
      ])('submits the main query and its refresh key with $type priority', async ({ cacheMode, queuePriority, expected }) => {
        const suffix = crypto.randomBytes(8).toString('hex');
        const mainQuery = `SELECT priority-main-${suffix}`;
        const cacheKeyQuery = `SELECT priority-refresh-key-${suffix}`;

        const querySpy = jest.spyOn(cache, 'queryWithRetryAndRelease').mockImplementation(async (query) => {
          if (query === mainQuery) {
            return [{ result: 'ok' }];
          }

          return [{ refresh_key: suffix }];
        });
        const renewCycleSpy = jest.spyOn(cache, 'startRenewCycle').mockImplementation(() => undefined);

        try {
          await cache.cachedQueryResult(
            {
              query: mainQuery,
              values: [],
              cacheMode,
              queuePriority,
              cacheKeyQueries: [[cacheKeyQuery, []]],
              requestId: `priority-req-${suffix}`,
              dataSource: 'default',
            },
            [],
          );

          const priorityOf = (targetQuery: string) => querySpy.mock.calls
            .filter(([query]) => query === targetQuery)
            .map(([, , queryOptions]) => queryOptions.priority);

          expect(priorityOf(mainQuery)).toEqual([expected]);
          expect(priorityOf(cacheKeyQuery)).toEqual([expected]);
        } finally {
          renewCycleSpy.mockRestore();
          querySpy.mockRestore();
        }
      });

      // The branch that bypasses the cache: `cacheKeyQueriesFrom` always returns an array, so
      // an empty `cacheKeyQueries` still renews — only these two query shapes reach it.
      it.each([
        { type: 'an external query that skips the cache and queue', queryBody: { external: true } },
        { type: 'a persistent query', queryBody: { persistent: true } },
      ])('submits $type with Interactive priority', async ({ queryBody }) => {
        const localCache = new QueryCacheOpened(
          crypto.randomBytes(16).toString('hex'),
          () => {
            throw new Error('driverFactory is not implemented, mock should be used...');
          },
          jest.fn(),
          { ...options, skipExternalCacheAndQueue: true },
        );
        const querySpy = jest.spyOn(localCache, 'queryWithRetryAndRelease')
          .mockImplementation(async () => [{ result: 'ok' }]);

        try {
          await localCache.cachedQueryResult(
            {
              ...queryBody,
              query: 'SELECT skip-cache-main',
              values: [],
              cacheKeyQueries: [],
              requestId: 'skip-cache-req',
              dataSource: 'default',
            },
            [],
          );

          expect(querySpy.mock.calls.map(([, , queryOptions]) => queryOptions.priority))
            .toEqual([QueuePriority.Interactive]);
        } finally {
          querySpy.mockRestore();
          await localCache.cleanup();
        }
      });
    });

    describe('local refresh key', () => {
      const REFRESH_KEY_SQL = 'SELECT FLOOR((UNIX_TIMESTAMP()) / 600) as refresh_key';
      const descriptor = { interval: 600, utcOffset: 0, dayOffset: 0, cron: false };

      const newCache = (additionalOptions: Partial<QueryCacheOptions> = {}) => (
        new QueryCacheOpened(
          crypto.randomBytes(16).toString('hex'),
          () => {
            throw new Error('driverFactory is not implemented, mock should be used...');
          },
          jest.fn(),
          { ...options, ...additionalOptions },
        )
      );

      const loadRefreshKey = async (
        queryOptions: any,
        additionalOptions: Partial<QueryCacheOptions> = {},
      ) => {
        const localCache = newCache(additionalOptions);
        const spy = jest.spyOn(localCache, 'queryWithRetryAndRelease')
          .mockImplementation(async () => [{ refresh_key: 12345 }]);

        try {
          const [result] = await Promise.all(
            localCache.loadRefreshKeys(
              [[REFRESH_KEY_SQL, [], queryOptions]],
              60,
              { dataSource: 'default' },
            )
          );

          return { result, executed: spy.mock.calls.length, logged: localCache.logger.mock.calls };
        } finally {
          spy.mockRestore();
          await localCache.cleanup();
        }
      };

      it('evaluates locally without touching the driver', async () => {
        const { result, executed } = await loadRefreshKey(
          { external: true, renewalThreshold: 60, localRefreshKey: descriptor },
          { localRefreshKey: true },
        );

        expect(executed).toBe(0);
        expect(result).toEqual([{ refresh_key: String(Math.floor(Date.now() / 1000 / 600)) }]);
      });

      it('runs the query when the flag is off', async () => {
        const { result, executed } = await loadRefreshKey(
          { external: true, renewalThreshold: 60, localRefreshKey: descriptor },
          { localRefreshKey: false },
        );

        expect(executed).toBe(1);
        expect(result).toEqual([{ refresh_key: 12345 }]);
      });

      it('runs the query when there is no descriptor', async () => {
        const { executed } = await loadRefreshKey(
          { external: false, renewalThreshold: 10 },
          { localRefreshKey: true },
        );

        expect(executed).toBe(1);
      });

      it('runs the query when the descriptor is malformed', async () => {
        const { executed } = await loadRefreshKey(
          { external: true, renewalThreshold: 60, localRefreshKey: { ...descriptor, interval: 0 } },
          { localRefreshKey: true },
        );

        expect(executed).toBe(1);
      });

      // Local evaluation would advance the key on every interval boundary, ignoring the
      // throttle a deployment asked for and multiplying pre-aggregation rebuilds.
      it('runs the query when refreshKeyRenewalThreshold is configured', async () => {
        const { result, executed } = await loadRefreshKey(
          { external: true, renewalThreshold: 60, localRefreshKey: descriptor },
          { localRefreshKey: true, refreshKeyRenewalThreshold: 24 * 60 * 60 },
        );

        expect(executed).toBe(1);
        expect(result).toEqual([{ refresh_key: 12345 }]);
      });

      // The refresh scheduler reads this to decide whether warming a refresh key is pointless,
      // so it has to agree with the branches above.
      it('reports whether local evaluation is in effect', async () => {
        const enabled = newCache({ localRefreshKey: true });
        const disabled = newCache({ localRefreshKey: false });
        const throttled = newCache({ localRefreshKey: true, refreshKeyRenewalThreshold: 24 * 60 * 60 });

        try {
          expect(enabled.isLocalRefreshKeyActive()).toBe(true);
          expect(disabled.isLocalRefreshKeyActive()).toBe(false);
          expect(throttled.isLocalRefreshKeyActive()).toBe(false);
        } finally {
          await Promise.all([enabled.cleanup(), disabled.cleanup(), throttled.cleanup()]);
        }
      });

      it('runs the query when the flag is unset', async () => {
        const { result, executed } = await loadRefreshKey({
          external: true,
          renewalThreshold: 60,
          localRefreshKey: descriptor,
        });

        expect(executed).toBe(1);
        expect(result).toEqual([{ refresh_key: 12345 }]);
      });

      // Falling back to the SQL path is intended for every declined branch, so nothing reports
      // it; the ordinary cache messages are still expected.
      it.each([
        { name: 'the flag is off', additionalOptions: { localRefreshKey: false } },
        { name: 'the flag is unset', additionalOptions: {} },
        {
          name: 'refreshKeyRenewalThreshold is configured',
          additionalOptions: { localRefreshKey: true, refreshKeyRenewalThreshold: 24 * 60 * 60 },
        },
      ])('does not report the declined local evaluation when $name', async ({ additionalOptions }) => {
        const { logged } = await loadRefreshKey(
          { external: true, renewalThreshold: 60, localRefreshKey: descriptor },
          additionalOptions,
        );

        expect(logged.map(([message]) => message).filter(m => /local/i.test(m))).toEqual([]);
      });

      describe('localRefreshKeyResult', () => {
        const withCache = async (
          additionalOptions: Partial<QueryCacheOptions>,
          fn: (localCache: QueryCacheOpened) => void,
        ) => {
          const localCache = newCache(additionalOptions);

          try {
            fn(localCache);
            expect(localCache.logger).not.toHaveBeenCalled();
          } finally {
            await localCache.cleanup();
          }
        };

        const queryOptions = (localRefreshKey?: unknown) => <any>{ localRefreshKey };

        it('evaluates a valid descriptor', () => withCache({ localRefreshKey: true }, localCache => {
          expect(localCache.localRefreshKeyResult(queryOptions(descriptor)))
            .toEqual([{ refresh_key: String(Math.floor(Date.now() / 1000 / 600)) }]);
        }));

        // Cube Store returns every column as a string, so a locally evaluated key has to be
        // a string too or flipping the flag invalidates every pre-aggregation once.
        it('returns the key as a string', () => withCache({ localRefreshKey: true }, localCache => {
          const [{ refresh_key: value }] = localCache
            .localRefreshKeyResult(queryOptions(descriptor))!;

          expect(typeof value).toBe('string');
        }));

        it('declines when the flag is off', () => withCache({ localRefreshKey: false }, localCache => {
          expect(localCache.localRefreshKeyResult(queryOptions(descriptor))).toBeNull();
        }));

        it('declines without a descriptor', () => withCache({ localRefreshKey: true }, localCache => {
          expect(localCache.localRefreshKeyResult()).toBeNull();
          expect(localCache.localRefreshKeyResult(queryOptions())).toBeNull();
        }));

        it('declines a malformed descriptor', () => withCache({ localRefreshKey: true }, localCache => {
          expect(localCache.localRefreshKeyResult(
            queryOptions({ ...descriptor, interval: 0 }),
          )).toBeNull();
        }));

        it('declines when refreshKeyRenewalThreshold is configured', () => withCache(
          { localRefreshKey: true, refreshKeyRenewalThreshold: 24 * 60 * 60 },
          localCache => {
            expect(localCache.localRefreshKeyResult(queryOptions(descriptor))).toBeNull();
          },
        ));
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
