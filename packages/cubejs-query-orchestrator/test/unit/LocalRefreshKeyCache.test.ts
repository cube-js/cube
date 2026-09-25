import crypto from 'crypto';
import { QueryCache, QueryWithParams } from '../../src';

// Keep queue timers real while advancing the clock used by cache entries and refresh keys.
const realTimers = [
  'hrtime', 'nextTick', 'performance', 'queueMicrotask', 'setImmediate', 'clearImmediate',
  'setInterval', 'clearInterval', 'setTimeout', 'clearTimeout',
] as const;

describe('local refresh key SQL cache compatibility', () => {
  const start = 97_800_000;
  const descriptor = { interval: 600, utcOffset: 0, dayOffset: 0 };
  const caches: QueryCache[] = [];

  beforeEach(() => jest.useFakeTimers({ now: start, doNotFake: [...realTimers] }));
  afterEach(async () => {
    jest.useRealTimers();
    jest.restoreAllMocks();
    await Promise.all(caches.splice(0).map(cache => cache.cleanup()));
  });

  const setup = (threshold = 86400, interval = 600) => {
    const q: QueryWithParams = [`SELECT FLOOR(UNIX_TIMESTAMP() / ${interval}) as refresh_key`, [], {
      localRefreshKey: { ...descriptor, interval },
    }];
    const make = (localRefreshKey: boolean, prefix = crypto.randomBytes(16).toString('hex')) => {
      // An independent stand-in for the SQL formula, executed by the real queue handler.
      const query = jest.fn(async () => [{ refresh_key: String(Math.floor(Date.now() / (interval * 1000))) }]);
      const factory = jest.fn(async () => ({ query } as any));
      const cache = new QueryCache(prefix, factory, jest.fn(), {
        cacheAndQueueDriver: 'memory',
        localRefreshKey,
        refreshKeyRenewalThreshold: threshold,
        queueOptions: async () => ({ concurrency: 2 }),
      });
      caches.push(cache);
      return { cache, factory, query };
    };
    return { q, make };
  };

  test.each([
    { name: 'daily TTL keeps the value after an hour', ttl: 86400, threshold: 86400, interval: 600, elapsed: 3600000, changes: false },
    { name: 'short TTL expires before the threshold', ttl: 60, threshold: 86400, interval: 60, elapsed: 120000, changes: true },
    { name: 'threshold is measured from the write time', ttl: 86400, threshold: 120, interval: 60, elapsed: 120001, changes: true },
    { name: 'does not renew early at a wall-clock threshold boundary', ttl: 86400, threshold: 120, interval: 60, elapsed: 83001, changes: false },
  ])('$name', async ({ ttl, threshold, interval, elapsed, changes }) => {
    const { q, make } = setup(threshold, interval);
    const sql = make(false);
    const local = make(true);
    const read = (cache: QueryCache) => cache.cacheRefreshKeyResult(q, ttl, { dataSource: 'default', waitForRenew: true });
    // Deliberately off any interval or renewal boundary.
    jest.setSystemTime(start + 37000);
    const before = await read(sql.cache);
    expect(await read(local.cache)).toEqual(before);
    jest.setSystemTime(Date.now() + elapsed);
    const after = await read(sql.cache);
    expect(await read(local.cache)).toEqual(after);
    if (changes) {
      expect(after).not.toEqual(before);
    } else {
      expect(after).toEqual(before);
    }
    expect(local.factory).not.toHaveBeenCalled();
  });

  test.each([3600, 86400])('readers with different TTLs retain the stored TTL %i', async firstTtl => {
    const { q, make } = setup();
    const secondTtl = firstTtl === 3600 ? 86400 : 3600;

    for (const localRefreshKey of [false, true]) {
      const prefix = crypto.randomBytes(16).toString('hex');
      const first = make(localRefreshKey, prefix);
      const second = make(localRefreshKey, prefix);
      const secondSet = jest.spyOn(second.cache.getCacheDriver(), 'set');
      jest.setSystemTime(start);
      const before = await first.cache.cacheRefreshKeyResult(q, firstTtl, { dataSource: 'default', waitForRenew: true });
      jest.setSystemTime(start + 600000);
      expect(await second.cache.cacheRefreshKeyResult(q, secondTtl, { dataSource: 'default', waitForRenew: true })).toEqual(before);
      expect(secondSet).not.toHaveBeenCalled();
      jest.setSystemTime(start + 3600001);
      const after = await second.cache.cacheRefreshKeyResult(q, secondTtl, { dataSource: 'default', waitForRenew: true });
      if (firstTtl === 3600) {
        expect(after).not.toEqual(before);
      } else {
        expect(after).toEqual(before);
      }
      if (localRefreshKey) {
        expect(first.factory).not.toHaveBeenCalled();
        expect(second.factory).not.toHaveBeenCalled();
      }
    }
  });

  test.each([false, true])('background renewal returns the stored value first (local=%s)', async localRefreshKey => {
    const { q, make } = setup(120, 60);
    const { cache } = make(localRefreshKey);
    const before = await cache.cacheRefreshKeyResult(q, 86400, { dataSource: 'default', waitForRenew: true });
    jest.setSystemTime(start + 120001);
    const driver = cache.getCacheDriver();
    const originalSet = driver.set.bind(driver);
    let releaseWrite: () => void;
    const writeAllowed = new Promise<void>(resolve => { releaseWrite = resolve; });
    let write: ReturnType<typeof driver.set> | undefined;
    const set = jest.spyOn(driver, 'set').mockImplementation((...args) => {
      write = writeAllowed.then(() => originalSet(...args));
      return write;
    });
    const key = cache.refreshKeyCacheKey(q, 'default');
    const returned = jest.fn();
    const read = cache.cacheRefreshKeyResult(q, 86400, { dataSource: 'default', waitForRenew: false }).then(returned);

    try {
      // Let the queue and promise callbacks run, keeping the renewal write blocked.
      await new Promise<void>(resolve => setImmediate(resolve));
      expect(returned).toHaveBeenCalledWith(before);
      expect(set).toHaveBeenCalledTimes(1);
      expect(await driver.get(key)).toMatchObject({ result: before });
    } finally {
      releaseWrite();
      await read;
      await write;
    }
    // Read storage directly so a second cache lookup cannot repair a failed renewal.
    expect(await driver.get(key)).toMatchObject({ result: [{ refresh_key: '1632' }] });
  });

  test.each(['get', 'set'] as const)('propagates cache %s failures without querying the source', async method => {
    const { q, make } = setup();
    const local = make(true);
    jest.spyOn(local.cache.getCacheDriver(), method).mockRejectedValueOnce(new Error('cache unavailable'));
    await expect(local.cache.cacheRefreshKeyResult(q, 3600, { dataSource: 'default', waitForRenew: true }))
      .rejects.toThrow('cache unavailable');
    expect(local.factory).not.toHaveBeenCalled();
  });

  test.each([false, true])('reuses entries across SQL/local mode changes (written locally=%s)', async writtenLocally => {
    const { q, make } = setup();
    const prefix = crypto.randomBytes(16).toString('hex');
    const writer = make(writtenLocally, prefix);
    const reader = make(!writtenLocally, prefix);
    const before = await writer.cache.cacheRefreshKeyResult(q, 86400, { dataSource: 'default', waitForRenew: true });
    jest.setSystemTime(start + 3600000);
    const enqueue = jest.spyOn(reader.cache, 'queryWithRetryAndRelease');
    expect(await reader.cache.cacheRefreshKeyResult(q, 3600, { dataSource: 'default', waitForRenew: true })).toEqual(before);
    expect(enqueue).not.toHaveBeenCalled();
    expect(reader.factory).not.toHaveBeenCalled();
  });

  test.each<{
    name: string;
    localRefreshKey: boolean;
    queryOptions: QueryWithParams[2];
  }>([
    { name: 'local evaluation is disabled', localRefreshKey: false, queryOptions: { localRefreshKey: descriptor } },
    { name: 'the descriptor is absent', localRefreshKey: true, queryOptions: {} },
    { name: 'the interval is invalid', localRefreshKey: true, queryOptions: { localRefreshKey: { ...descriptor, interval: 0 } } },
    { name: 'the key is incremental', localRefreshKey: true, queryOptions: { localRefreshKey: descriptor, incremental: true } },
  ])('executes SQL under a threshold when $name', async ({ localRefreshKey, queryOptions }) => {
    const { q, make } = setup();
    const { cache, query } = make(localRefreshKey);
    const sqlQuery: QueryWithParams = [q[0], q[1], queryOptions];
    expect(await cache.cacheRefreshKeyResult(sqlQuery, 3600, { dataSource: 'default', waitForRenew: true }))
      .toEqual([{ refresh_key: '163' }]);
    expect(query).toHaveBeenCalledTimes(1);
  });

  test.each([0, undefined])('without a threshold override uses neither the cache nor the queue (%s)', async threshold => {
    const { q, make } = setup();
    const local = make(true);
    local.cache.options.refreshKeyRenewalThreshold = threshold;
    const get = jest.spyOn(local.cache.getCacheDriver(), 'get');
    const set = jest.spyOn(local.cache.getCacheDriver(), 'set');
    const enqueue = jest.spyOn(local.cache, 'queryWithRetryAndRelease');
    expect(await local.cache.cacheRefreshKeyResult(q, 60, { dataSource: 'default' })).toEqual([{ refresh_key: '163' }]);
    jest.setSystemTime(start + 600000);
    expect(await local.cache.cacheRefreshKeyResult(q, 60, { dataSource: 'default' })).toEqual([{ refresh_key: '164' }]);
    expect(get).not.toHaveBeenCalled();
    expect(set).not.toHaveBeenCalled();
    expect(enqueue).not.toHaveBeenCalled();
    expect(local.factory).not.toHaveBeenCalled();
  });
});
