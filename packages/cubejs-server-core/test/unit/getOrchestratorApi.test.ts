// A replaced `OrchestratorStorage` entry is released, and releasing an api closes
// its Cube Store connection for good -- so building an api twice for one id
// destroys the connection of whoever holds the api it replaces.

import { CubejsServerCore } from '../../src';
import { OrchestratorApi } from '../../src/core/OrchestratorApi';

const cores: CubejsServerCore[] = [];

function createServerCore(options: Record<string, unknown> = {}) {
  const core = new CubejsServerCore(<any>{
    apiSecret: 'secret',
    driverFactory: () => <any>({ type: 'postgres' }),
    // One id for every caller: a burst of requests carrying the same security
    // context, which is what the deployment that reported this was serving.
    contextToOrchestratorId: () => 'ORCHESTRATOR_ID',
    ...options,
  });

  cores.push(core);

  return core;
}

async function callConcurrently(core: CubejsServerCore, times: number) {
  return Promise.all(
    Array.from({ length: times }, (_, i) => core.getOrchestratorApi({
      requestId: `request-${i}`,
      authInfo: null,
      securityContext: null,
    } as any))
  );
}

// `disposeAfter` releases asynchronously, so let the microtasks drain first.
const flushReleases = () => new Promise(resolve => { setImmediate(resolve); });

async function waitFor(condition: () => boolean) {
  for (let i = 0; i < 100 && !condition(); i++) {
    await flushReleases();
  }

  if (!condition()) {
    throw new Error('Timed out waiting for the build to reach its gate');
  }
}

describe('CubejsServerCore.getOrchestratorApi', () => {
  let release: jest.SpyInstance;

  beforeEach(() => {
    // Releasing is what closes the Cube Store web socket, so counting these
    // calls counts the connections destroyed.
    release = jest.spyOn(OrchestratorApi.prototype, 'release')
      .mockImplementation(async () => undefined);
  });

  afterEach(async () => {
    // In `afterEach` rather than at the end of each test: `releaseConnections()`
    // is what cancels the scheduled refresh timer the constructor starts, and a
    // test that fails before its last line would otherwise leave it running.
    await Promise.all(cores.splice(0).map(core => core.releaseConnections()));

    release.mockRestore();
  });

  test('concurrent callers of one id share a single orchestrator api', async () => {
    const apis = await callConcurrently(createServerCore(), 5);

    expect(new Set(apis).size).toEqual(1);
  });

  test('no orchestrator handed to a caller is released behind its back', async () => {
    await callConcurrently(createServerCore(), 5);

    expect(release).not.toHaveBeenCalled();
  });

  // One `/v1/load` with `total: true` runs its data query and its count query
  // through `Promise.all`, each fetching the api for itself, so a single request
  // is enough to race with itself.
  test('the two queries of one total:true request get the same api', async () => {
    const [dataQuery, countQuery] = await callConcurrently(createServerCore(), 2);

    expect(dataQuery).toBe(countQuery);
    expect(release).not.toHaveBeenCalled();
  });

  test('callers of different ids get an api each', async () => {
    const core = createServerCore({
      contextToOrchestratorId: (context: any) => context.requestId,
    });

    const apis = await callConcurrently(core, 3);

    // The other cases all pin one id, so a memo keyed too loosely -- or not
    // keyed at all -- would satisfy every one of them.
    expect(new Set(apis).size).toEqual(3);
    expect(release).not.toHaveBeenCalled();
  });

  test('a later caller reuses the cached api rather than building another', async () => {
    const core = createServerCore();

    const [first] = await callConcurrently(core, 3);
    const later = await callConcurrently(core, 3);

    expect(later.every(api => api === first)).toBe(true);
    expect(release).not.toHaveBeenCalled();
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
  });

  // Reachable only when the build dropped by `resetInstanceState()` fails: one
  // that succeeds fills the cache on its way out, and callers read the cache
  // before the memo. Deleting the entry it finds would send the replacement
  // build's callers back to building an api each.
  test('a build that outlives a reset does not drop the build that replaced it', async () => {
    const core = createServerCore();
    const gates: Array<() => void> = [];
    let builds = 0;

    jest.spyOn(core as any, 'orchestratorOptions').mockImplementation(async () => {
      const build = builds++;

      // Only the two builds this test orchestrates are held: a third one is the
      // defect, and letting it run to completion makes the assertions below
      // report the duplicate rather than time out waiting on it.
      if (build < 2) {
        await new Promise<void>(resolve => { gates.push(resolve); });
      }

      if (build === 0) {
        throw new Error('the deployment went away mid-build');
      }

      return {};
    });

    const acrossReset = callConcurrently(core, 1);
    await waitFor(() => gates.length === 1);

    await core.resetInstanceState();

    const afterReset = callConcurrently(core, 1);
    await waitFor(() => gates.length === 2);

    gates[0]();
    await expect(acrossReset).rejects.toThrow('the deployment went away mid-build');

    // The replacement is still in flight and the cache is still empty, so this
    // caller can only be answered by the memo entry the failure just ran past.
    const later = callConcurrently(core, 1);
    gates[1]();

    expect((await later)[0]).toBe((await afterReset)[0]);
    expect(builds).toEqual(2);
    expect(release).not.toHaveBeenCalled();
  });

  test('the Cube Store driver of a live caller is not closed under it', async () => {
    // The step that turns a released api into the reported error: `release()`
    // closes the external driver, and that close is terminal.
    release.mockRestore();

    const closed: number[] = [];
    let drivers = 0;
    const core = createServerCore({
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

    expect(closed).toEqual([]);
    expect(new Set(apis).size).toEqual(1);
  });
});
