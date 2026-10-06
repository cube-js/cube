import { PostgresQuery } from '../../../src/adapter/PostgresQuery';
import { prepareYamlCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

// https://github.com/cube-js/cube/issues/7420
// Indexes on an `original_sql` pre-aggregation are generated against member aliases
// (`base_positions__position_id`), but an `original_sql` table holds the raw columns of
// the cube's SQL (`id`), so `CREATE INDEX` fails with `column ... does not exist`.
describe('Issue 7420: indexes on original_sql pre-aggregations', () => {
  jest.setTimeout(200000);

  const { compiler, joinGraph, cubeEvaluator } = prepareYamlCompiler(`
cubes:
  - name: base_positions
    sql: SELECT * FROM visitors
    measures:
      - name: count
        type: count
    dimensions:
      - name: position_id
        sql: id
        type: number
        primary_key: true
      - name: source
        sql: source
        type: string
      - name: created_at
        sql: created_at
        type: time
    pre_aggregations:
      - name: main
        type: original_sql
        external: false
        indexes:
          - name: position_id_index
            columns:
              - position_id
`);

  it('builds an index on the original_sql table and answers a query from it', async () => {
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: ['base_positions.count'],
      dimensions: ['base_positions.source'],
      order: [{ id: 'base_positions.source' }],
      timezone: 'UTC',
      preAggregationsSchema: '',
    });

    query.buildSqlAndParams();
    const preAggregationsDescription: any = query.preAggregations?.preAggregationsDescription();
    expect(preAggregationsDescription[0].type).toEqual('originalSql');

    expect(preAggregationsDescription[0].indexesSql.length).toEqual(1);

    // Builds the original_sql table, runs its `CREATE INDEX`, then queries it.
    const res = await dbRunner.evaluateQueryWithPreAggregations(query);
    expect(res).toEqual([
      { base_positions__source: 'google', base_positions__count: '1' },
      { base_positions__source: 'some', base_positions__count: '2' },
      { base_positions__source: null, base_positions__count: '3' },
    ]);
  });
});
