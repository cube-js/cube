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
  });
});
