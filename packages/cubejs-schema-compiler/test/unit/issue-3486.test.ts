import { prepareJsCompiler } from './PrepareCompiler';

// https://github.com/cube-js/cube/issues/3486
// A rollup pre-aggregation that references the same member twice compiles fine,
// and builds a table with duplicate columns that Cube Store later fails to plan
// ("Schema contains duplicate qualified field name"). Compilation should fail instead.
describe('Issue #3486: pre-aggregation with non-unique member references', () => {
  it('rejects a rollup that references the same dimension twice', async () => {
    const { compiler } = prepareJsCompiler(`
      cube('orders', {
        sql: 'SELECT * FROM orders',
        measures: {
          count: { type: 'count' },
        },
        dimensions: {
          id: { sql: 'id', type: 'number', primaryKey: true },
          status: { sql: 'status', type: 'string' },
        },
        preAggregations: {
          main: {
            measures: [CUBE.count],
            dimensions: [CUBE.status, CUBE.status],
          },
        },
      });
    `);

    await expect(async () => {
      await compiler.compile();
      compiler.throwIfAnyErrors();
    }).rejects.toThrow();
  });
});
