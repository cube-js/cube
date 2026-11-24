import { QueryOrchestrator } from '../../src';

describe('QueryOrchestrator streamQuery', () => {
  const streamQuery = async (rollupOnlyMode: boolean, preAggregationsTablesToTempTables: [string, any][]) => {
    const orchestrator = Object.create(QueryOrchestrator.prototype);
    orchestrator.rollupOnlyMode = rollupOnlyMode;
    orchestrator.preAggregations = {
      loadAllPreAggregationsIfNeeded: async () => ({ preAggregationsTablesToTempTables, values: null as string[] | null }),
    };
    orchestrator.queryCache = {
      cachedQueryResult: jest.fn(async () => 'stream'),
    };

    return orchestrator.streamQuery({ query: 'SELECT 1', values: [] });
  };

  test('rollup only mode rejects a streamed query that reads the data source', async () => {
    await expect(streamQuery(true, [])).rejects.toThrow('rollup only mode');
  });

  test('rollup only mode streams a query served from a pre-aggregation in the data source', async () => {
    await expect(streamQuery(true, [['orders_rollup', { targetTableName: 'orders_rollup_abc' }]]))
      .resolves.toEqual('stream');
  });

  test('streams from the data source outside rollup only mode', async () => {
    await expect(streamQuery(false, [])).resolves.toEqual('stream');
  });
});
