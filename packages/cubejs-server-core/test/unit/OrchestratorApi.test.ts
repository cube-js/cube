import { PassThrough } from 'stream';

import { OrchestratorApi } from '../../src/core/OrchestratorApi';

// The constructor builds a real QueryOrchestrator, so the fields it would set up
// are stubbed instead.
const buildApi = (): any => {
  const api: any = Object.create(OrchestratorApi.prototype);

  api.seenDataSources = {};
  api.holders = 0;
  api.drained = [];
  api.logger = jest.fn();
  api.options = {
    contextToDbType: jest.fn(async () => 'postgres'),
    contextToExternalDbType: jest.fn(() => 'cubestore'),
  };
  api.continueWaitTimeout = 30;
  api.holdersLinger = 0;
  api.externalDriverSeen = false;
  api.driverFactory = jest.fn();
  api.orchestrator = { cleanup: jest.fn(async () => undefined) };

  return api;
};

const tick = () => new Promise((resolve) => { setTimeout(resolve, 10); });

describe('OrchestratorApi', () => {
  // https://github.com/cube-js/cube/issues/11313
  test('getPreAggregationQueueStates forwards dataSource to the orchestrator', async () => {
    const api = buildApi();
    const getPreAggregationQueueStates = jest.fn(async () => []);
    api.orchestrator.getPreAggregationQueueStates = getPreAggregationQueueStates;

    await api.getPreAggregationQueueStates('test_ds');
    expect(getPreAggregationQueueStates).toHaveBeenLastCalledWith('test_ds');

    // QueryOrchestrator#getPreAggregationQueueStates defaults an undefined
    // dataSource to 'default', so calls without one keep working.
    await api.getPreAggregationQueueStates();
    expect(getPreAggregationQueueStates).toHaveBeenLastCalledWith(undefined);
  });

  describe('holders', () => {
    test('release waits for the work that is already running', async () => {
      const api = buildApi();
      const releaseHolder = api.acquire();

      let done = false;
      const releasing = api.release().then(() => { done = true; });

      await tick();

      expect(done).toBe(false);
      expect(api.orchestrator.cleanup).not.toHaveBeenCalled();

      releaseHolder();
      await releasing;

      expect(api.orchestrator.cleanup).toHaveBeenCalledTimes(1);
    });

    test('release does not wait when nothing is running', async () => {
      const api = buildApi();

      await api.release();

      expect(api.orchestrator.cleanup).toHaveBeenCalledTimes(1);
    });

    test('releasing a holder twice does not let the release through early', async () => {
      const api = buildApi();
      const first = api.acquire();
      const second = api.acquire();

      first();
      first();

      let done = false;
      const releasing = api.release().then(() => { done = true; });
      await tick();

      expect(done).toBe(false);

      second();
      await releasing;

      expect(api.orchestrator.cleanup).toHaveBeenCalledTimes(1);
    });

    test('a query in flight holds off the release', async () => {
      const api = buildApi();
      let answer: (result: any) => void = () => undefined;
      api.orchestrator.fetchQuery = jest.fn(() => new Promise((resolve) => { answer = resolve; }));

      const query = api.executeQuery({ requestId: 'test' });
      await tick();

      let done = false;
      const releasing = api.release().then(() => { done = true; });
      await tick();

      expect(done).toBe(false);

      answer({ data: [] });
      await query;
      await releasing;

      expect(api.orchestrator.cleanup).toHaveBeenCalledTimes(1);
    });

    test('a stream holds off the release until it is done', async () => {
      const api = buildApi();
      const result = new PassThrough();
      api.orchestrator.streamQuery = jest.fn(async () => result);

      await api.streamQuery({ requestId: 'test' });

      let done = false;
      const releasing = api.release().then(() => { done = true; });
      await tick();

      // The stream outlives the call that returned it.
      expect(done).toBe(false);

      result.end();
      result.resume();
      await releasing;

      expect(api.orchestrator.cleanup).toHaveBeenCalledTimes(1);
    });

    test('waits out the gap between the calls one operation makes', async () => {
      const api = buildApi();
      api.holdersLinger = 100;

      const first = api.acquire();

      let done = false;
      const releasing = api.release().then(() => { done = true; });

      // The count reaching zero is not the end of the work: the caller is
      // between two calls on the same api.
      first();
      const second = api.acquire();

      await tick();

      expect(done).toBe(false);

      second();
      await releasing;

      expect(api.orchestrator.cleanup).toHaveBeenCalledTimes(1);
    });

    test('a release that waited does not stay on the timer queue', async () => {
      const api = buildApi();
      const releaseHolder = api.acquire();
      const timers = jest.spyOn(global, 'clearTimeout');

      const releasing = api.release();
      await tick();
      releaseHolder();
      await releasing;

      expect(timers).toHaveBeenCalled();
      // The resolver of the race it won is gone too, so a later drain has
      // nothing to call.
      expect(api.drained).toHaveLength(0);

      timers.mockRestore();
    });

    test('a shutdown does not wait for the work in progress', async () => {
      const api = buildApi();
      api.acquire();

      await api.release({ waitForWork: false });

      expect(api.orchestrator.cleanup).toHaveBeenCalledTimes(1);
    });
  });

  describe('external driver', () => {
    test('is not created just to be released', async () => {
      const externalDriverFactory = jest.fn(async () => ({ release: jest.fn() }));
      const api = buildApi();
      api.externalDriverFactory = externalDriverFactory;

      await api.release();

      expect(externalDriverFactory).not.toHaveBeenCalled();
    });

    test('is released once it has been created', async () => {
      const externalDriver = { release: jest.fn(async () => undefined) };
      const api = buildApi();
      api.externalDriverFactory = jest.fn(async () => {
        api.externalDriverSeen = true;

        return externalDriver;
      });

      await api.externalDriverFactory();
      await api.release();

      expect(externalDriver.release).toHaveBeenCalledTimes(1);
    });
  });
});
