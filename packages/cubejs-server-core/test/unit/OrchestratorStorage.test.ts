import { OrchestratorApi } from '../../src/core/OrchestratorApi';
import { OrchestratorStorage } from '../../src/core/OrchestratorStorage';

/**
 * `OrchestratorStorage` only ever calls `release()` on what it holds, so the
 * entry is stubbed down to that: the release lifecycle inside `OrchestratorApi`
 * is its own unit.
 */
function createApi(release: () => Promise<unknown> = async () => undefined) {
  const api = { release: jest.fn(release) };

  return { api: api as unknown as OrchestratorApi, release: api.release };
}

/**
 * A real `OrchestratorApi` with a stubbed `QueryOrchestrator`, for the one case
 * that is about what the api logs rather than what the storage does with it.
 */
function createRealApi(driverError: Error) {
  const logger = jest.fn();
  const externalDriver = { release: async () => { throw driverError; } };

  const api = new OrchestratorApi(
    async () => externalDriver as any,
    logger,
    {
      cacheAndQueueDriver: 'memory',
      contextToDbType: async () => 'postgres',
      contextToExternalDbType: () => 'cubestore',
      externalDriverFactory: async () => externalDriver as any,
      redisPrefix: 'tenant-1',
    }
  );

  (api as any).orchestrator = { cleanup: async () => undefined };

  return { api, logger };
}

// lru-cache runs `disposeAfter` once the operation that removed the entry has
// finished, and the release it schedules is asynchronous.
const flush = () => new Promise(resolve => setImmediate(resolve));

describe('OrchestratorStorage', () => {
  test('releases an orchestrator the LRU evicts, and keeps the one still cached', async () => {
    const storage = new OrchestratorStorage({ compilerCacheSize: 1 });
    const evicted = createApi();
    const kept = createApi();

    storage.set('evicted', evicted.api);
    storage.set('kept', kept.api);
    await flush();

    expect(storage.has('evicted')).toBe(false);
    expect(evicted.release).toHaveBeenCalledTimes(1);

    expect(storage.has('kept')).toBe(true);
    expect(kept.release).not.toHaveBeenCalled();
  });

  test('releases an orchestrator replaced under the same id', async () => {
    const storage = new OrchestratorStorage({ compilerCacheSize: 10 });
    const first = createApi();
    const second = createApi();

    storage.set('same-id', first.api);
    storage.set('same-id', second.api);
    await flush();

    expect(first.release).toHaveBeenCalledTimes(1);
    expect(second.release).not.toHaveBeenCalled();
  });

  test('setting the same instance again does not release it', async () => {
    const storage = new OrchestratorStorage({ compilerCacheSize: 10 });
    const { api, release } = createApi();

    storage.set('same-id', api);
    storage.set('same-id', api);
    await flush();

    expect(storage.has('same-id')).toBe(true);
    expect(release).not.toHaveBeenCalled();
  });

  test('releaseConnections waits for the releases it schedules', async () => {
    const storage = new OrchestratorStorage({ compilerCacheSize: 10 });
    let finishRelease: () => void = () => undefined;
    const { api, release } = createApi(() => new Promise<void>(resolve => {
      finishRelease = () => resolve();
    }));

    storage.set('a', api);

    let settled = false;
    const releasing = storage.releaseConnections().then(() => { settled = true; });
    await flush();

    // `clear()` only schedules the releases, so shutdown has to wait for them:
    // returning here would let the process exit with the connections still open.
    expect(release).toHaveBeenCalledTimes(1);
    expect(settled).toBe(false);

    finishRelease();
    await releasing;

    expect(settled).toBe(true);
    expect(storage.has('a')).toBe(false);
  });

  test('releaseConnections resolves and clears even when a release fails', async () => {
    const storage = new OrchestratorStorage({ compilerCacheSize: 10 });
    const failing = createApi(async () => { throw new Error('driver is gone'); });
    const ok = createApi();

    storage.set('failing', failing.api);
    storage.set('ok', ok.api);

    await expect(storage.releaseConnections()).resolves.toBeUndefined();

    expect(ok.release).toHaveBeenCalledTimes(1);
    expect(storage.has('failing')).toBe(false);
    expect(storage.has('ok')).toBe(false);
  });

  test('a release that fails is logged rather than lost', async () => {
    const storage = new OrchestratorStorage({ compilerCacheSize: 1 });
    const { api, logger } = createRealApi(new Error('driver is gone'));

    storage.set('failing', api);
    storage.set('next', createApi().api);
    await flush();

    // The storage swallows the rejection so shutdown cannot fail on it, which
    // leaves the log as the only signal that a connection stayed open.
    expect(logger).toHaveBeenCalledWith(
      'Orchestrator Release Error',
      expect.objectContaining({
        orchestratorId: 'tenant-1',
        error: expect.stringContaining('driver is gone'),
      })
    );
  });

  test('forgets a release once it has finished', async () => {
    const storage = new OrchestratorStorage({ compilerCacheSize: 1 });

    storage.set('a', createApi().api);
    storage.set('b', createApi().api);
    await storage.releaseConnections();
    await flush();

    // Otherwise the set grows by one entry per eviction -- a smaller leak in
    // place of the one being fixed.
    expect((storage as any).pendingReleases.size).toBe(0);
  });
});
