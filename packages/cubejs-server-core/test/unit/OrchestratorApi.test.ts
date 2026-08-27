import { OrchestratorApi } from '../../src/core/OrchestratorApi';

describe('OrchestratorApi', () => {
  // https://github.com/cube-js/cube/issues/11313
  test('getPreAggregationQueueStates forwards dataSource to the orchestrator', async () => {
    const api = Object.create(OrchestratorApi.prototype);
    const getPreAggregationQueueStates = jest.fn(async () => []);
    api.orchestrator = { getPreAggregationQueueStates };

    await api.getPreAggregationQueueStates('test_ds');
    expect(getPreAggregationQueueStates).toHaveBeenLastCalledWith('test_ds');

    // QueryOrchestrator#getPreAggregationQueueStates defaults an undefined
    // dataSource to 'default', so calls without one keep working.
    await api.getPreAggregationQueueStates();
    expect(getPreAggregationQueueStates).toHaveBeenLastCalledWith(undefined);
  });

  describe('release', () => {
    function createApi() {
      const externalRelease = jest.fn(async () => undefined);
      const externalDriver = { release: externalRelease };
      const externalDriverFactory = jest.fn(async () => externalDriver as any);

      const api = new OrchestratorApi(
        async () => ({ release: jest.fn() }) as any,
        jest.fn(),
        {
          cacheAndQueueDriver: 'memory',
          contextToDbType: async () => 'postgres',
          contextToExternalDbType: () => 'cubestore',
          externalDriverFactory,
        }
      );

      (api as any).orchestrator = { cleanup: async () => undefined };

      return { api, externalDriverFactory, externalRelease };
    }

    test('does not build the external driver just to close it', async () => {
      const { api, externalDriverFactory, externalRelease } = createApi();

      await api.release();

      // The factory creates the connection, so calling it here would have every
      // eviction open a Cube Store connection purely to close it again.
      expect(externalDriverFactory).not.toHaveBeenCalled();
      expect(externalRelease).not.toHaveBeenCalled();
    });

    test('closes the external driver once something has built it', async () => {
      const { api, externalRelease } = createApi();

      // What the orchestrator does the first time it touches a pre-aggregation.
      await (api as any).options.externalDriverFactory();

      await api.release();

      expect(externalRelease).toHaveBeenCalledTimes(1);
    });
  });
});
