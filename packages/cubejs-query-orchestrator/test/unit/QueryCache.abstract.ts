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
      const FRESH_RESULT = { data: 'fresh' };
      const CACHED_RESULT = { data: 'cached' };
      const RENEWAL_THRESHOLD = 120;
      const EXPIRATION = 600;

      let loggerCalls: [string, any][];
      let fetchNewSpy: jest.SpyInstance;

      beforeEach(() => {
        loggerCalls = [];

        // Replace logger to capture calls
        (cache as any).logger = (msg: string, params: any) => {
          loggerCalls.push([msg, params]);
        };

        fetchNewSpy = jest.spyOn(cache as any, 'queryWithRetryAndRelease')
          .mockResolvedValue(FRESH_RESULT);
      });

      afterEach(() => {
        fetchNewSpy.mockRestore();
      });

      const seedCache = async (redisKey: string, time: number, renewalKey: string) => {
        await (cache as any).cacheDriver.set(redisKey, {
          time,
          result: CACHED_RESULT,
          renewalKey,
        }, EXPIRATION);
      };

      it('cache expired + waitForRenew → blocks on fetchNew', async () => {
        const cacheKey = ['select 1', ['v1'], [], []] as any;
        const redisKey = cache.queryRedisKey(cacheKey);
        const oldRenewalKey = cache.queryRedisKey(['old-key', 'pad'] as any);
        const newRenewalKey = ['new-key', 'pad'];

        // Seed with time older than renewalThreshold
        await seedCache(redisKey, Date.now() - (RENEWAL_THRESHOLD + 10) * 1000, oldRenewalKey);

        const result = await cache.cacheQueryResult(
          'select 1',
          ['v1'],
          cacheKey,
          EXPIRATION,
          {
            renewalThreshold: RENEWAL_THRESHOLD,
            renewalKey: newRenewalKey,
            waitForRenew: true,
            dataSource: 'default',
          },
        );

        expect(result).toEqual(FRESH_RESULT);
        expect(fetchNewSpy).toHaveBeenCalled();
        const waitMsg = loggerCalls.find(([msg]) => msg === 'Waiting for renew');
        expect(waitMsg).toBeTruthy();
      });

      it('cache expired + no waitForRenew → returns cached, background refresh', async () => {
        const cacheKey = ['select 2', ['v2'], [], []] as any;
        const redisKey = cache.queryRedisKey(cacheKey);
        const oldRenewalKey = cache.queryRedisKey(['old-key-2', 'pad'] as any);
        const newRenewalKey = ['new-key-2', 'pad'];

        await seedCache(redisKey, Date.now() - (RENEWAL_THRESHOLD + 10) * 1000, oldRenewalKey);

        const result = await cache.cacheQueryResult(
          'select 2',
          ['v2'],
          cacheKey,
          EXPIRATION,
          {
            renewalThreshold: RENEWAL_THRESHOLD,
            renewalKey: newRenewalKey,
            waitForRenew: false,
            dataSource: 'default',
          },
        );

        // Returns the cached result immediately
        expect(result).toEqual(CACHED_RESULT);
        // fetchNew is still called in background
        expect(fetchNewSpy).toHaveBeenCalled();
        const renewMsg = loggerCalls.find(([msg]) => msg === 'Renewing existing key');
        expect(renewMsg).toBeTruthy();
      });

      it('key mismatch, not expired, user request → returns cached, background refresh', async () => {
        const cacheKey = ['select 3', ['v3'], [], []] as any;
        const redisKey = cache.queryRedisKey(cacheKey);
        const oldRenewalKey = cache.queryRedisKey(['old-key-3', 'pad'] as any);
        const newRenewalKey = ['new-key-3', 'pad'];

        // Recent time - within threshold
        await seedCache(redisKey, Date.now() - 10 * 1000, oldRenewalKey);

        const result = await cache.cacheQueryResult(
          'select 3',
          ['v3'],
          cacheKey,
          EXPIRATION,
          {
            renewalThreshold: RENEWAL_THRESHOLD,
            renewalKey: newRenewalKey,
            waitForRenew: true,
            renewCycle: false, // user request, not a renew cycle
            dataSource: 'default',
          },
        );

        expect(result).toEqual(CACHED_RESULT);
        expect(fetchNewSpy).toHaveBeenCalled();
        const bgMsg = loggerCalls.find(([msg]) => msg === 'Renewing key in background (key mismatch, not expired)');
        expect(bgMsg).toBeTruthy();
      });

      it('key mismatch, not expired, renew cycle → blocks on fetchNew', async () => {
        const cacheKey = ['select 4', ['v4'], [], []] as any;
        const redisKey = cache.queryRedisKey(cacheKey);
        const oldRenewalKey = cache.queryRedisKey(['old-key-4', 'pad'] as any);
        const newRenewalKey = ['new-key-4', 'pad'];

        // Recent time - within threshold
        await seedCache(redisKey, Date.now() - 10 * 1000, oldRenewalKey);

        const result = await cache.cacheQueryResult(
          'select 4',
          ['v4'],
          cacheKey,
          EXPIRATION,
          {
            renewalThreshold: RENEWAL_THRESHOLD,
            renewalKey: newRenewalKey,
            waitForRenew: true,
            renewCycle: true, // background renew cycle
            dataSource: 'default',
          },
        );

        expect(result).toEqual(FRESH_RESULT);
        expect(fetchNewSpy).toHaveBeenCalled();
        const waitMsg = loggerCalls.find(([msg]) => msg === 'Waiting for renew (key mismatch, renew cycle)');
        expect(waitMsg).toBeTruthy();
      });

      it('key matches, not expired → returns cached, no fetchNew', async () => {
        const cacheKey = ['select 5', ['v5'], [], []] as any;
        const redisKey = cache.queryRedisKey(cacheKey);
        const renewalKey = ['same-key', 'pad'];
        const renewalKeyRedis = cache.queryRedisKey(renewalKey as any);

        // Recent time with matching key
        await seedCache(redisKey, Date.now() - 10 * 1000, renewalKeyRedis);

        const result = await cache.cacheQueryResult(
          'select 5',
          ['v5'],
          cacheKey,
          EXPIRATION,
          {
            renewalThreshold: RENEWAL_THRESHOLD,
            renewalKey,
            waitForRenew: true,
            dataSource: 'default',
          },
        );

        expect(result).toEqual(CACHED_RESULT);
        expect(fetchNewSpy).not.toHaveBeenCalled();
        // Should find "Using cache for" but no renewal messages
        const usingCacheMsg = loggerCalls.find(([msg]) => msg === 'Using cache for');
        expect(usingCacheMsg).toBeTruthy();
        const renewMsg = loggerCalls.find(([msg]) => msg.includes('Renew') || msg.includes('Waiting'));
        expect(renewMsg).toBeFalsy();
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
