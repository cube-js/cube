import { PostgresQuery } from '../../src';
import { prepareYamlCompiler } from './PrepareCompiler';

const DAY = 24 * 60 * 60;

const schema = (refreshKey: string) => `
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
${refreshKey}
`;

// https://github.com/cube-js/cube/issues/3586
describe('refreshKey.updateWindow', () => {
  it.each([
    ['7 day', 7 * DAY],
    ['2 weeks', 14 * DAY],
    // Calendar units resolve to their longest length, so a partition is never sealed
    // before the calendar window that the SQL refresh key checks has passed
    ['1 month', 31 * DAY],
    ['3 months', 93 * DAY],
    ['1 quarter', 92 * DAY],
    ['1 year', 366 * DAY],
  ])('accepts update_window: %s', async (updateWindow, seconds) => {
    const compilers = prepareYamlCompiler(schema(`
          every: 1 day
          incremental: true
          update_window: ${updateWindow}`));

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
    const [description] = query.newPreAggregations().preAggregationsDescription();
    expect(description.updateWindowSeconds).toEqual(seconds);
  });

  it('still rejects calendar units in refresh_key.every', async () => {
    const compilers = prepareYamlCompiler(schema(`
          every: 1 month`));

    await compilers.compiler.compile().catch(() => undefined);
    expect(() => compilers.compiler.throwIfAnyErrors()).toThrow(/every = 1 month/);
  });
});
