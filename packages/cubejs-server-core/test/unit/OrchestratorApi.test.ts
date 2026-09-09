import { ContinueWaitError } from '@cubejs-backend/query-orchestrator';

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

  describe('streamQuery', () => {
    const buildApi = (streamQuery: jest.Mock) => {
      const api = Object.create(OrchestratorApi.prototype);
      api.orchestrator = { streamQuery };
      api.logger = jest.fn();
      return api;
    };

    // A pre-aggregation still building reaches `streamQuery` as a
    // `ContinueWaitError`. The gateway turns it into a retryable 200 by testing
    // `err.message === 'Continue wait'` (gateway.ts), and cubesql retries only
    // on that exact string (scan.rs `eq_ignore_ascii_case("continue wait")`).
    // Wrapping it into `{ error: ... }` would drop `message`, so the query
    // would fail with a 400 instead of waiting for the build.
    test('lets a ContinueWaitError through unwrapped, so the SQL API still retries', async () => {
      const api = buildApi(jest.fn().mockRejectedValue(new ContinueWaitError()));

      await expect(api.streamQuery({ query: 'SELECT 1' })).rejects.toThrow(ContinueWaitError);

      const err = await api.streamQuery({ query: 'SELECT 1' }).catch((e: any) => e);
      expect(err.message).toBe('Continue wait');

      // Logged as a wait, not as a failure — matching what `executeQuery` does
      // for the same condition.
      expect(api.logger).toHaveBeenCalledWith('Continue wait', expect.anything());
      expect(api.logger).not.toHaveBeenCalledWith('Error querying db', expect.anything());
    });

    // Every other failure keeps the shape `executeQuery` rejects with, so the
    // gateway reports a query error rather than an Internal Server Error.
    test('wraps any other error the way executeQuery does', async () => {
      const api = buildApi(jest.fn().mockRejectedValue(new Error('Boom')));

      const err = await api.streamQuery({ query: 'SELECT 1' }).catch((e: any) => e);
      expect(err).toEqual({ error: 'Error: Boom' });
      expect(api.logger).toHaveBeenCalledWith('Error querying db', expect.anything());
    });

    test('returns the stream untouched when nothing throws', async () => {
      const _stream = { pipe: jest.fn() };
      const api = buildApi(jest.fn().mockResolvedValue(_stream));

      await expect(api.streamQuery({ query: 'SELECT 1' })).resolves.toBe(_stream);
      expect(api.logger).not.toHaveBeenCalled();
    });
  });
});
