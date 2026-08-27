import { OrchestratorStorage } from '../../src/core/OrchestratorStorage';
import type { OrchestratorApi } from '../../src/core/OrchestratorApi';

const mockApi = () => {
  const release = jest.fn(async () => []);
  return { api: { release } as unknown as OrchestratorApi, release };
};

describe('OrchestratorStorage', () => {
  test('releases an api evicted from the cache', async () => {
    const storage = new OrchestratorStorage({ compilerCacheSize: 1 });
    const evicted = mockApi();
    const kept = mockApi();

    storage.set('evicted', evicted.api);
    storage.set('kept', kept.api);

    await storage.releaseConnections();

    expect(evicted.release).toHaveBeenCalledTimes(1);
    expect(kept.release).toHaveBeenCalledTimes(1);
  });

  test('releases an api replaced under the same key', async () => {
    const storage = new OrchestratorStorage({ compilerCacheSize: 10 });
    const replaced = mockApi();

    storage.set('id', replaced.api);
    storage.set('id', mockApi().api);

    await storage.releaseConnections();

    expect(replaced.release).toHaveBeenCalledTimes(1);
  });

  test('releaseConnections releases every api exactly once', async () => {
    const storage = new OrchestratorStorage({ compilerCacheSize: 10 });
    const first = mockApi();
    const second = mockApi();

    storage.set('first', first.api);
    storage.set('second', second.api);

    await storage.releaseConnections();
    // Shutdown calls it more than once (resetInstanceState, then shutdown itself).
    await storage.releaseConnections();

    expect(first.release).toHaveBeenCalledTimes(1);
    expect(second.release).toHaveBeenCalledTimes(1);
  });

  test('a failing release is reported and does not reject releaseConnections', async () => {
    const storage = new OrchestratorStorage({ compilerCacheSize: 1 });
    const error = new Error('dead tenant');
    const failing = {
      release: jest.fn(async () => { throw error; })
    } as unknown as OrchestratorApi;
    const reported = jest.spyOn(console, 'error').mockImplementation(() => undefined);

    try {
      storage.set('failing', failing);
      storage.set('other', mockApi().api);

      await expect(storage.releaseConnections()).resolves.toBeUndefined();
      expect(reported).toHaveBeenCalledWith(expect.any(String), error);
    } finally {
      reported.mockRestore();
    }
  });
});
