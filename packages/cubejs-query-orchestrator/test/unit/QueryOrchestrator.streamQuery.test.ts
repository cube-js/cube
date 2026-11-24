import { LoadPreAggregationResult, QueryOrchestrator } from '../../src';

class TestQueryOrchestrator extends QueryOrchestrator {
  public stubPreAggregations(preAggregationsTablesToTempTables: [string, LoadPreAggregationResult][]) {
    jest.spyOn(this.preAggregations, 'loadAllPreAggregationsIfNeeded')
      .mockResolvedValue({ preAggregationsTablesToTempTables, values: null });
    jest.spyOn(this.queryCache, 'cachedQueryResult').mockResolvedValue('stream');
  }
}

describe('QueryOrchestrator streamQuery', () => {
  const streamQuery = async (
    rollupOnlyMode: boolean,
    preAggregationsTablesToTempTables: [string, LoadPreAggregationResult][],
  ) => {
    const orchestrator = new TestQueryOrchestrator(
      'STREAM_QUERY_TEST',
      async () => {
        throw new Error('No driver expected');
      },
      () => undefined,
      { cacheAndQueueDriver: 'memory', rollupOnlyMode },
    );
    orchestrator.stubPreAggregations(preAggregationsTablesToTempTables);

    try {
      return await orchestrator.streamQuery({ query: 'SELECT 1', values: [] });
    } finally {
      await orchestrator.cleanup();
    }
  };

  const rollupInDataSource: [string, LoadPreAggregationResult] = [
    'orders_rollup',
    { targetTableName: 'orders_rollup_abc' } as LoadPreAggregationResult,
  ];

  test('rollup only mode rejects a streamed query that reads the data source', async () => {
    await expect(streamQuery(true, [])).rejects.toThrow('rollup only mode');
  });

  test('rollup only mode streams a query served from a pre-aggregation in the data source', async () => {
    await expect(streamQuery(true, [rollupInDataSource])).resolves.toEqual('stream');
  });

  test('streams from the data source outside rollup only mode', async () => {
    await expect(streamQuery(false, [])).resolves.toEqual('stream');
  });
});
