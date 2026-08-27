import { OrchestratorStorage } from '../../src/core/OrchestratorStorage';
import type { OrchestratorApi } from '../../src/core/OrchestratorApi';

const mockApi = () => {
  const release = jest.fn(async () => []);
  return { api: { release } as unknown as OrchestratorApi, release };
};

// Lets a scheduled release fire without waiting out a real delay.
const settle = () => new Promise((resolve) => { setTimeout(resolve, 10); });

describe('OrchestratorStorage', () => {
  test('releases an api evicted from the cache', async () => {
    const storage = new OrchestratorStorage({ compilerCacheSize: 1, releaseDelay: 0 });
    const evicted = mockApi();

    storage.set('evicted', evicted.api);
    storage.set('kept', mockApi().api);

    await settle();

    expect(evicted.release).toHaveBeenCalledTimes(1);
  });

  test('releases an api replaced under the same key', async () => {
    const storage = new OrchestratorStorage({ compilerCacheSize: 10, releaseDelay: 0 });
    const replaced = mockApi();

    storage.set('id', replaced.api);
    storage.set('id', mockApi().api);

    await settle();

    expect(replaced.release).toHaveBeenCalledTimes(1);
  });

  test('keeps an evicted api alive for the release delay', async () => {
    const storage = new OrchestratorStorage({ compilerCacheSize: 1, releaseDelay: 60 * 1000 });
    const evicted = mockApi();

    storage.set('evicted', evicted.api);
    storage.set('kept', mockApi().api);

    // A request that was handed this api before the eviction is still using it.
    await settle();

    expect(evicted.release).not.toHaveBeenCalled();

    // Shutdown doesn't wait the delay out.
    await storage.releaseConnections();

    expect(evicted.release).toHaveBeenCalledTimes(1);
  });

  test('releaseConnections releases every api exactly once', async () => {
    const storage = new OrchestratorStorage({ compilerCacheSize: 10, releaseDelay: 0 });
    const first = mockApi();
    const second = mockApi();

    storage.set('first', first.api);
    storage.set('second', second.api);

    await storage.releaseConnections();
    // Shutdown calls it more than once (resetInstanceState, then shutdown itself).
    await storage.releaseConnections();
    await settle();

    expect(first.release).toHaveBeenCalledTimes(1);
    expect(second.release).toHaveBeenCalledTimes(1);
  });

  test('a failing release is reported and does not stop the others', async () => {
    const storage = new OrchestratorStorage({ compilerCacheSize: 10, releaseDelay: 0 });
    const error = new Error('dead tenant');
    const failing = {
      release: jest.fn(async () => { throw error; })
    } as unknown as OrchestratorApi;
    const other = mockApi();
    const reported = jest.spyOn(console, 'error').mockImplementation(() => undefined);

    try {
      storage.set('failing', failing);
      storage.set('other', other.api);

      await expect(storage.releaseConnections()).resolves.toBeUndefined();

      expect(reported).toHaveBeenCalledWith(expect.any(String), error);
      expect(other.release).toHaveBeenCalledTimes(1);
    } finally {
      reported.mockRestore();
    }
  });
});
