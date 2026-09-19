import { CacheAction, CacheEntry, CacheQueryResultOptions, QueryCache } from '../../src';
import { QueryCacheTest } from './QueryCache.abstract';

QueryCacheTest('Local', {
  cacheAndQueueDriver: 'memory',
  backgroundRenew: false,
});

class QueryCacheOpened extends QueryCache {
  public static get disablePeriod(): number {
    return this.IN_MEMORY_CACHE_DISABLE_PERIOD;
  }

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

  public static isMemoryEntryUsable(
    entry: Partial<CacheEntry>,
    renewedAgo: number,
    expiration: number,
    renewalThreshold?: number,
    renewalKey?: string,
  ): boolean {
    return super.isMemoryEntryUsable(
      entry as CacheEntry,
      renewedAgo,
      expiration,
      renewalThreshold,
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
      expected: CacheAction.ServeCached,
    },
    {
      name: 'expired with renewal key and waitForRenew: blocks on fetch',
      entry: { time: 1, renewalKey: 'rk', requestId: 'req-1' },
      renewedAgo: EXPIRED,
      options: { renewalThreshold: THRESHOLD, requestId: 'req-2', waitForRenew: true },
      renewalKey: 'rk',
      expected: CacheAction.WaitForRenew,
    },
    {
      name: 'expired with renewal key without waitForRenew: refreshes in background',
      entry: { time: 1, renewalKey: 'rk', requestId: 'req-1' },
      renewedAgo: EXPIRED,
      options: { renewalThreshold: THRESHOLD, requestId: 'req-2', waitForRenew: false },
      renewalKey: 'rk',
      expected: CacheAction.RefreshBackground,
    },
    {
      name: 'renewal key mismatch while not expired and waitForRenew: blocks on fetch',
      entry: { time: 1, renewalKey: 'old', requestId: 'req-1' },
      renewedAgo: NOT_EXPIRED,
      options: { renewalThreshold: THRESHOLD, requestId: 'req-2', waitForRenew: true },
      renewalKey: 'new',
      expected: CacheAction.WaitForRenew,
    },
    {
      name: 'renewal key mismatch while not expired without waitForRenew: refreshes in background',
      entry: { time: 1, renewalKey: 'old', requestId: 'req-1' },
      renewedAgo: NOT_EXPIRED,
      options: { renewalThreshold: THRESHOLD, requestId: 'req-2', waitForRenew: false },
      renewalKey: 'new',
      expected: CacheAction.RefreshBackground,
    },
    {
      name: 'same request (different span) and expired: serves stale, refreshes in background',
      entry: { time: 1, renewalKey: 'rk', requestId: 'req-1-span-1' },
      renewedAgo: EXPIRED,
      options: { renewalThreshold: THRESHOLD, requestId: 'req-1-span-7', waitForRenew: true },
      renewalKey: 'rk',
      expected: CacheAction.RefreshSameRequest,
    },
    {
      name: 'same request and renewal key mismatch: serves stale, refreshes in background',
      entry: { time: 1, renewalKey: 'old', requestId: 'req-1-span-1' },
      renewedAgo: NOT_EXPIRED,
      options: { renewalThreshold: THRESHOLD, requestId: 'req-1-span-7', waitForRenew: true },
      renewalKey: 'new',
      expected: CacheAction.RefreshSameRequest,
    },
    {
      name: 'renew cycle never serves stale: expired same request blocks on fetch',
      entry: { time: 1, renewalKey: 'rk', requestId: 'req-1-span-1' },
      renewedAgo: EXPIRED,
      options: { renewalThreshold: THRESHOLD, requestId: 'req-1-span-7', waitForRenew: true, renewCycle: true },
      renewalKey: 'rk',
      expected: CacheAction.WaitForRenew,
    },
    {
      name: 'renew cycle never serves stale: key mismatch without waitForRenew refreshes in background',
      entry: { time: 1, renewalKey: 'old', requestId: 'req-1-span-1' },
      renewedAgo: NOT_EXPIRED,
      options: { renewalThreshold: THRESHOLD, requestId: 'req-1-span-7', waitForRenew: false, renewCycle: true },
      renewalKey: 'new',
      expected: CacheAction.RefreshBackground,
    },
    {
      name: 'expired without a renewal key: keeps serving cache',
      entry: { time: 1, requestId: 'req-1' },
      renewedAgo: EXPIRED,
      options: { renewalThreshold: THRESHOLD, requestId: 'req-2', waitForRenew: true },
      expected: CacheAction.ServeCached,
    },
    {
      name: 'expired without a renewal key but same request: refreshes in background',
      entry: { time: 1, requestId: 'req-1-span-1' },
      renewedAgo: EXPIRED,
      options: { renewalThreshold: THRESHOLD, requestId: 'req-1-span-7', waitForRenew: true },
      expected: CacheAction.RefreshSameRequest,
    },
    {
      name: 'missing renewal threshold counts as expired',
      entry: { time: 1, renewalKey: 'rk', requestId: 'req-1' },
      renewedAgo: 0,
      options: { requestId: 'req-2', waitForRenew: true },
      renewalKey: 'rk',
      expected: CacheAction.WaitForRenew,
    },
    {
      name: 'entry without a timestamp counts as expired',
      entry: { time: 0, renewalKey: 'rk', requestId: 'req-1' },
      renewedAgo: 0,
      options: { renewalThreshold: THRESHOLD, requestId: 'req-2', waitForRenew: false },
      renewalKey: 'rk',
      expected: CacheAction.RefreshBackground,
    },
    {
      name: 'entry without a request id is never treated as the same request',
      entry: { time: 1, renewalKey: 'rk' },
      renewedAgo: EXPIRED,
      options: { renewalThreshold: THRESHOLD, requestId: 'req-1', waitForRenew: true },
      renewalKey: 'rk',
      expected: CacheAction.WaitForRenew,
    },
  ];

  cases.forEach(({ name, entry, renewedAgo, options, renewalKey, expected }) => {
    it(name, () => {
      expect(QueryCacheOpened.decideCacheAction(entry, renewedAgo, options, renewalKey)).toEqual(expected);
    });
  });
});

describe('QueryCache.isMemoryEntryUsable', () => {
  const WINDOW = QueryCacheOpened.disablePeriod;
  const HOUR = 3600;

  type Case = {
    name: string,
    entry: Partial<CacheEntry>,
    renewedAgo: number,
    expiration: number,
    renewalThreshold?: number,
    renewalKey?: string,
    expected: boolean,
  };

  const cases: Case[] = [
    {
      name: 'past expiration but still inside the in-memory window: unusable',
      entry: { time: 1 },
      renewedAgo: 61 * 1000,
      expiration: 60,
      expected: false,
    },
    {
      name: 'past the in-memory window but nowhere near expiration: unusable',
      entry: { time: 1 },
      renewedAgo: WINDOW + 1,
      expiration: HOUR,
      expected: false,
    },
    {
      name: 'exactly at the in-memory window: usable',
      entry: { time: 1 },
      renewedAgo: WINDOW,
      expiration: HOUR,
      expected: true,
    },
    {
      name: 'exactly at expiration: usable',
      entry: { time: 1 },
      renewedAgo: 60 * 1000,
      expiration: 60,
      expected: true,
    },
    {
      name: 'without a renewal key the renewal checks are skipped: no threshold and no timestamp still usable',
      entry: { time: 0 },
      renewedAgo: 1000,
      expiration: HOUR,
      expected: true,
    },
    {
      name: 'renewal key without a renewal threshold: unusable',
      entry: { time: 1, renewalKey: 'rk' },
      renewedAgo: 1000,
      expiration: HOUR,
      renewalKey: 'rk',
      expected: false,
    },
    {
      name: 'renewal key with an entry that has no timestamp: unusable',
      entry: { time: 0, renewalKey: 'rk' },
      renewedAgo: 1000,
      expiration: HOUR,
      renewalThreshold: HOUR,
      renewalKey: 'rk',
      expected: false,
    },
    {
      name: 'threshold below twice the window, entry inside the last window before expiry: unusable',
      entry: { time: 1, renewalKey: 'rk' },
      renewedAgo: 100 * 1000,
      expiration: HOUR,
      renewalThreshold: 300,
      renewalKey: 'rk',
      expected: false,
    },
    {
      name: 'threshold below twice the window, entry exactly at the last window before expiry: usable',
      entry: { time: 1, renewalKey: 'rk' },
      renewedAgo: 100 * 1000,
      expiration: HOUR,
      renewalThreshold: 400,
      renewalKey: 'rk',
      expected: true,
    },
    {
      name: 'renewal key mismatch: unusable',
      entry: { time: 1, renewalKey: 'old' },
      renewedAgo: 1000,
      expiration: HOUR,
      renewalThreshold: HOUR,
      renewalKey: 'new',
      expected: false,
    },
    {
      name: 'every check satisfied: usable',
      entry: { time: 1, renewalKey: 'rk' },
      renewedAgo: 1000,
      expiration: HOUR,
      renewalThreshold: HOUR,
      renewalKey: 'rk',
      expected: true,
    },
  ];

  cases.forEach(({ name, entry, renewedAgo, expiration, renewalThreshold, renewalKey, expected }) => {
    it(name, () => {
      expect(
        QueryCacheOpened.isMemoryEntryUsable(entry, renewedAgo, expiration, renewalThreshold, renewalKey)
      ).toEqual(expected);
    });
  });
});
