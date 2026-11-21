import { PostgresQuery } from '../../src';
import { prepareYamlCompiler } from './PrepareCompiler';

const DAY = 24 * 60 * 60;

// https://github.com/cube-js/cube/issues/3586
describe('Issue #3586 - refreshKey.updateWindow with month / quarter / year', () => {
  it.each([
    // Lower bounds: the shortest possible month / quarter / year
    ['1 month', 28 * DAY],
    ['3 months', 89 * DAY],
    ['1 quarter', 89 * DAY],
    ['1 year', 365 * DAY],
  ])('accepts update_window: %s', async (updateWindow, minSeconds) => {
    const compilers = prepareYamlCompiler(`
cubes:
  - name: orders
    sql_table: orders

    measures:
      - name: count
        type: count

    dimensions:
      - name: created_at
        sql: created_at
        type: time

    pre_aggregations:
      - name: main
        measures:
          - count
        time_dimension: created_at
        granularity: day
        partition_granularity: month
        refresh_key:
          every: 1 day
          incremental: true
          update_window: ${updateWindow}
`);

    await compilers.compile();
    compilers.compiler.throwIfAnyErrors();

    const query = compilers.createQuery(PostgresQuery, {
      measures: ['orders.count'],
      timeDimensions: [{
        dimension: 'orders.created_at',
        granularity: 'day',
        dateRange: ['2024-01-01', '2024-01-31'],
      }],
      timezone: 'UTC',
    });

    // The window must also be usable at refresh time, not only pass validation
    const [description]: any = query.newPreAggregations().preAggregationsDescription();
    expect(description.updateWindowSeconds).toBeGreaterThanOrEqual(minSeconds);
  });
});
