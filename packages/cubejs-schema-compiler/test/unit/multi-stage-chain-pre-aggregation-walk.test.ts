import { PostgresQuery } from '../../src/adapter/PostgresQuery';
import { prepareYamlCompiler } from './PrepareCompiler';

// The pre-aggregation matcher walks a chain of multi-stage measures to build
// its with-queries. Visiting each child twice per level made the walk cost
// 2^depth, so a 22-deep chain took over a minute per request.
describe('Multi-stage chain pre-aggregation walk', () => {
  const DEPTH = 16;

  const chainMeasures = Array.from({ length: DEPTH }, (_, i) => `
      - name: d${i + 1}
        type: number
        multi_stage: true
        sql: "{d${i}} + 0"`).join('');

  const model = `
cubes:
  - name: orders
    sql: "SELECT 1 AS id, 'a' AS region, 10 AS amount"
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: region
        sql: region
        type: string
    measures:
      - name: d0
        type: sum
        sql: amount
${chainMeasures}
    pre_aggregations:
      - name: base_by_region
        measures:
          - d0
        dimensions:
          - region
`;

  it('visits each chain member a bounded number of times', async () => {
    const { compiler, joinGraph, cubeEvaluator } = prepareYamlCompiler(model);
    await compiler.compile();
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [`orders.d${DEPTH}`],
      dimensions: ['orders.region'],
      timezone: 'UTC',
      useNativeSqlPlanner: false,
    });

    const walk = jest.spyOn(PostgresQuery.prototype, 'multiStageWithQueries');
    query.buildSqlAndParams();
    query.preAggregations.canUseTransformedQuery();

    expect(walk.mock.calls.length).toBeLessThan(DEPTH * 10);
    walk.mockRestore();
  });
});
