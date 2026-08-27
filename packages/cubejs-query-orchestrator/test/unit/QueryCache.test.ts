import { CacheAction, CacheEntry, CacheQueryResultOptions, QueryCache } from '../../src';
import { QueryCacheTest } from './QueryCache.abstract';

QueryCacheTest('Local', {
  cacheAndQueueDriver: 'memory',
});

/**
 * Opens up the protected decision helper and accepts partial fixtures, so the decision
 * table can be covered without seeding a cache driver.
 */
class QueryCacheOpened extends QueryCache {
  public static decideCacheAction(
    entry: Partial<CacheEntry>,
    renewedAgo: number,
    options: Partial<CacheQueryResultOptions>,
    renewalKey?: string,
  ): CacheAction {
    return super.decideCacheAction(
      entry as CacheEntry,
      renewedAgo,
      options as CacheQueryResultOptions,
      renewalKey,
    );
  }
}

describe('QueryCache.decideCacheAction', () => {
  const THRESHOLD = 600;
  const NOT_EXPIRED = 100 * 1000;
  const EXPIRED = 700 * 1000;

  type Case = {
    name: string,
    entry: Partial<CacheEntry>,
    renewedAgo: number,
    options: Partial<CacheQueryResultOptions>,
    renewalKey?: string,
    expected: CacheAction,
  };

  const cases: Case[] = [
    {
      name: 'not expired and renewal key matches: serves cache even for the same request',
      entry: { time: 1, renewalKey: 'rk', requestId: 'req-1-span-1' },
      renewedAgo: NOT_EXPIRED,
      options: { renewalThreshold: THRESHOLD, requestId: 'req-1-span-2', waitForRenew: true },
      renewalKey: 'rk',
      expected: 'serve-cached',
    },
    {
      name: 'expired with renewal key and waitForRenew: blocks on fetch',
      entry: { time: 1, renewalKey: 'rk', requestId: 'req-1' },
      renewedAgo: EXPIRED,
      options: { renewalThreshold: THRESHOLD, requestId: 'req-2', waitForRenew: true },
      renewalKey: 'rk',
      expected: 'wait-for-renew',
    },
    {
      name: 'expired with renewal key without waitForRenew: refreshes in background',
      entry: { time: 1, renewalKey: 'rk', requestId: 'req-1' },
      renewedAgo: EXPIRED,
      options: { renewalThreshold: THRESHOLD, requestId: 'req-2', waitForRenew: false },
      renewalKey: 'rk',
      expected: 'refresh-background',
    },
    {
      name: 'renewal key mismatch while not expired and waitForRenew: blocks on fetch',
      entry: { time: 1, renewalKey: 'old', requestId: 'req-1' },
      renewedAgo: NOT_EXPIRED,
      options: { renewalThreshold: THRESHOLD, requestId: 'req-2', waitForRenew: true },
      renewalKey: 'new',
      expected: 'wait-for-renew',
    },
    {
      name: 'renewal key mismatch while not expired without waitForRenew: refreshes in background',
      entry: { time: 1, renewalKey: 'old', requestId: 'req-1' },
      renewedAgo: NOT_EXPIRED,
      options: { renewalThreshold: THRESHOLD, requestId: 'req-2', waitForRenew: false },
      renewalKey: 'new',
      expected: 'refresh-background',
    },
    {
      name: 'same request (different span) and expired: serves stale, refreshes in background',
      entry: { time: 1, renewalKey: 'rk', requestId: 'req-1-span-1' },
      renewedAgo: EXPIRED,
      options: { renewalThreshold: THRESHOLD, requestId: 'req-1-span-7', waitForRenew: true },
      renewalKey: 'rk',
      expected: 'refresh-same-request',
    },
    {
      name: 'same request and renewal key mismatch: serves stale, refreshes in background',
      entry: { time: 1, renewalKey: 'old', requestId: 'req-1-span-1' },
      renewedAgo: NOT_EXPIRED,
      options: { renewalThreshold: THRESHOLD, requestId: 'req-1-span-7', waitForRenew: true },
      renewalKey: 'new',
      expected: 'refresh-same-request',
    },
    {
      name: 'renew cycle never serves stale: expired same request blocks on fetch',
      entry: { time: 1, renewalKey: 'rk', requestId: 'req-1-span-1' },
      renewedAgo: EXPIRED,
      options: { renewalThreshold: THRESHOLD, requestId: 'req-1-span-7', waitForRenew: true, renewCycle: true },
      renewalKey: 'rk',
      expected: 'wait-for-renew',
    },
    {
      name: 'renew cycle never serves stale: key mismatch without waitForRenew refreshes in background',
      entry: { time: 1, renewalKey: 'old', requestId: 'req-1-span-1' },
      renewedAgo: NOT_EXPIRED,
      options: { renewalThreshold: THRESHOLD, requestId: 'req-1-span-7', waitForRenew: false, renewCycle: true },
      renewalKey: 'new',
      expected: 'refresh-background',
    },
    {
      name: 'expired without a renewal key: keeps serving cache',
      entry: { time: 1, requestId: 'req-1' },
      renewedAgo: EXPIRED,
      options: { renewalThreshold: THRESHOLD, requestId: 'req-2', waitForRenew: true },
      expected: 'serve-cached',
    },
    {
      name: 'expired without a renewal key but same request: refreshes in background',
      entry: { time: 1, requestId: 'req-1-span-1' },
      renewedAgo: EXPIRED,
      options: { renewalThreshold: THRESHOLD, requestId: 'req-1-span-7', waitForRenew: true },
      expected: 'refresh-same-request',
    },
    {
      name: 'missing renewal threshold counts as expired',
      entry: { time: 1, renewalKey: 'rk', requestId: 'req-1' },
      renewedAgo: 0,
      options: { requestId: 'req-2', waitForRenew: true },
      renewalKey: 'rk',
      expected: 'wait-for-renew',
    },
    {
      name: 'entry without a timestamp counts as expired',
      entry: { time: 0, renewalKey: 'rk', requestId: 'req-1' },
      renewedAgo: 0,
      options: { renewalThreshold: THRESHOLD, requestId: 'req-2', waitForRenew: false },
      renewalKey: 'rk',
      expected: 'refresh-background',
    },
    {
      name: 'entry without a request id is never treated as the same request',
      entry: { time: 1, renewalKey: 'rk' },
      renewedAgo: EXPIRED,
      options: { renewalThreshold: THRESHOLD, requestId: 'req-1', waitForRenew: true },
      renewalKey: 'rk',
      expected: 'wait-for-renew',
    },
  ];

  cases.forEach(({ name, entry, renewedAgo, options, renewalKey, expected }) => {
    it(name, () => {
      expect(QueryCacheOpened.decideCacheAction(entry, renewedAgo, options, renewalKey)).toEqual(expected);
    });
  });
});
