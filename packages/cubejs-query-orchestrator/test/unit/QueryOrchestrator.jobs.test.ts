/* eslint-disable @typescript-eslint/no-explicit-any */
import { QueryOrchestrator } from '../../src';

// A jobed build returns one entry per built pre-aggregation — the requested partition plus
// every dependency it had to build first — and each entry becomes its own polling token.
// https://github.com/cube-js/cube/issues/11615
describe('QueryOrchestrator jobed build', () => {
  const entry = (tableName: string, overrides: Record<string, any> = {}) => ([
    tableName,
    {
      targetTableName: `${tableName}_kjypcoio_5yftl5il_1593709044209`,
      refreshKeyValues: [],
      lastUpdatedAt: 1593709044209,
      ...overrides,
    },
  ]);

  const fetchJob = async (preAggregationsTablesToTempTables: any[], preAggregations: any[]) => {
    const orchestrator = Object.create(QueryOrchestrator.prototype);
    orchestrator.rollupOnlyMode = false;
    orchestrator.preAggregations = {
      loadAllPreAggregationsIfNeeded: async () => ({
        preAggregationsTablesToTempTables,
        values: null,
      }),
    };

    return orchestrator.fetchQuery({ isJob: true, preAggregations });
  };

  test('labels every entry with its own pre-aggregation and data source', async () => {
    const job = await fetchJob(
      [
        entry('stb_pre_aggregations.orders_main', { preAggregationId: 'Orders.main', dataSource: 'orders_ds' }),
        entry('stb_pre_aggregations.orders_rollup', { preAggregationId: 'Orders.rollup', dataSource: 'default', timezone: 'UTC' }),
      ],
      [{ preAggregationId: 'Orders.main' }, { preAggregationId: 'Orders.rollup' }],
    );

    expect(job).toMatchObject([
      { preAggregation: 'Orders.main', tableName: 'stb_pre_aggregations.orders_main', dataSource: 'orders_ds' },
      { preAggregation: 'Orders.rollup', tableName: 'stb_pre_aggregations.orders_rollup', dataSource: 'default', timezone: 'UTC' },
    ]);
  });

  test('falls back to the requested pre-aggregation for an entry without an id', async () => {
    const job = await fetchJob(
      [entry('stb_pre_aggregations.orders_rollup')],
      [{ preAggregationId: 'Orders.rollup' }],
    );

    expect(job[0].preAggregation).toEqual('Orders.rollup');
  });
});
