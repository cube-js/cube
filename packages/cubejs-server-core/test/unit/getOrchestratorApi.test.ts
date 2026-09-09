import { CubejsServerCore } from '../../src';
import { OrchestratorApi } from '../../src/core/OrchestratorApi';

/**
 * `getOrchestratorApi()` is asynchronous and only writes the cache at the end,
 * so concurrent callers for one id used to miss the cache together and each
 * cache an api of its own. Every one of those writes replaces the entry, and a
 * replaced entry is released -- which closes the Cube Store connection of the
 * api a caller is about to run its query on. It surfaced as
 * `ConnectionError: Cube Store connection is closed` on requests that had done
 * nothing wrong.
 *
 * Concurrency of two is the everyday case rather than a rarity: a single
 * `/v1/load` with `total: true` runs its data query and its count query through
 * `Promise.all`, each fetching the orchestrator api for itself
 * (`gateway.ts` -> `getSqlResponseInternal`).
 */
async function callConcurrently(core: CubejsServerCore, times: number) {
  return Promise.all(
    Array.from({ length: times }, (_, i) => core.getOrchestratorApi({
      requestId: `request-${i}`,
      authInfo: null,
      securityContext: null,
    } as any))
  );
}

/**
 * `OrchestratorStorage` releases through lru-cache's `disposeAfter`, which runs
 * once the `set` that removed the entry has returned, and the release itself is
 * asynchronous -- so the connections close a few microtasks after the call that
 * cost them.
 */
const flushReleases = () => new Promise(resolve => { setImmediate(resolve); });

function createServerCore(options: Record<string, unknown> = {}) {
  return new CubejsServerCore(<any>{
    apiSecret: 'secret',
    driverFactory: () => <any>({ type: 'postgres' }),
    // One id for every caller: a burst of requests carrying the same security
    // context, which is what the deployment that reported this was serving.
    contextToOrchestratorId: () => 'ORCHESTRATOR_ID',
    ...options,
  });
}

describe('CubejsServerCore.getOrchestratorApi', () => {
  let release: jest.SpyInstance;

  beforeEach(() => {
    // Releasing is what closes the Cube Store web socket, and that close is
    // terminal, so counting these calls counts the connections destroyed.
    release = jest.spyOn(OrchestratorApi.prototype, 'release')
      .mockImplementation(async () => undefined);
  });

  afterEach(() => {
    release.mockRestore();
  });

  test('concurrent callers of one id share a single orchestrator api', async () => {
    const core = createServerCore();

    const apis = await callConcurrently(core, 5);

    expect(new Set(apis).size).toEqual(1);

    await core.releaseConnections();
  });

  test('no orchestrator handed to a caller is released behind its back', async () => {
    const core = createServerCore();

    await callConcurrently(core, 5);

    expect(release).not.toHaveBeenCalled();

    await core.releaseConnections();
  });

  test('the two queries of one total:true request get the same api', async () => {
    const core = createServerCore();

    const [dataQuery, countQuery] = await callConcurrently(core, 2);

    expect(dataQuery).toBe(countQuery);
    expect(release).not.toHaveBeenCalled();

    await core.releaseConnections();
  });

  test('a later caller reuses the cached api rather than building another', async () => {
    const core = createServerCore();

    const [first] = await callConcurrently(core, 3);
    const later = await callConcurrently(core, 3);

    expect(later.every(api => api === first)).toBe(true);
    expect(release).not.toHaveBeenCalled();

    await core.releaseConnections();
  });

  test('a failed build is not left behind to fail every later request', async () => {
    const core = createServerCore();
    let attempts = 0;

    jest.spyOn(core as any, 'orchestratorOptions').mockImplementation(async () => {
      attempts += 1;

      if (attempts === 1) {
        throw new Error('orchestrator options are not available yet');
      }

      return {};
    });

    await expect(callConcurrently(core, 3)).rejects.toThrow('orchestrator options are not available yet');

    // All three shared the one failing build, and the failure did not become
    // the cached answer for this id.
    expect(attempts).toEqual(1);
    await expect(callConcurrently(core, 1)).resolves.toBeDefined();
    expect(attempts).toEqual(2);

    await core.releaseConnections();
  });

  test('the Cube Store driver of a live caller is not closed under it', async () => {
    // The step that turns a released api into the error the deployment saw:
    // `release()` closes the external driver, and closing the Cube Store web
    // socket is terminal, so the caller still holding that api fails its next
    // query with `Cube Store connection is closed`.
    release.mockRestore();

    const closed: number[] = [];
    let drivers = 0;
    const core = createServerCore({
      externalDbType: 'cubestore',
      externalDriverFactory: () => {
        const id = drivers++;

        return {
          testConnection: async () => undefined,
          release: async () => { closed.push(id); },
        };
      },
    });

    const apis = await callConcurrently(core, 3);
    await flushReleases();

    // Every caller has to be left with a usable connection. Two of these three
    // used to be released, each closing the connection of a caller that had
    // just been handed it.
    expect(closed).toEqual([]);
    expect(new Set(apis).size).toEqual(1);

    await core.releaseConnections();
  });
});
