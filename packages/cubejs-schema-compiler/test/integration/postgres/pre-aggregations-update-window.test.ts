import { PostgresQuery } from '../../../src/adapter/PostgresQuery';
import { prepareYamlCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

// https://github.com/cube-js/cube/issues/3586
describe('PreAggregations refreshKey.updateWindow with calendar units', () => {
  jest.setTimeout(200000);

  it.each([
    '3 months',
    '1 quarter',
    '1 year',
  ])('builds and refreshes an incremental partitioned rollup with update_window: %s', async (updateWindow) => {
    const compilers = prepareYamlCompiler(`
cubes:
  - name: visitors_uw
    sql_table: visitors

    measures:
      - name: count
        type: count

    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true

      - name: created_at
        sql: created_at
        type: time

    pre_aggregations:
      - name: partitioned
        measures:
          - count
        time_dimension: created_at
        granularity: month
        partition_granularity: month
        refresh_key:
          every: 1 hour
          incremental: true
          update_window: ${updateWindow}
`);

    await compilers.compile();

    const query = compilers.createQuery(PostgresQuery, {
      measures: ['visitors_uw.count'],
      timeDimensions: [{
        dimension: 'visitors_uw.created_at',
        granularity: 'month',
        dateRange: ['2017-01-01', '2017-01-31'],
      }],
      timezone: 'UTC',
      preAggregationsSchema: '',
    });

    const [description]: any = query.preAggregations?.preAggregationsDescription();
    expect(description.tableName).toMatch(/partitioned/);
    // The incremental refresh key adds the window to the partition end in SQL, so evaluating
    // the pre-aggregation runs it against Postgres
    expect(description.invalidateKeyQueries[0][0]).toMatch(/NOW\(\) </);

    const res = await dbRunner.evaluateQueryWithPreAggregations(query);
    expect(res).toEqual([{
      visitors_uw__count: '5',
      visitors_uw__created_at_month: '2017-01-01T00:00:00.000Z',
    }]);
  });
});
