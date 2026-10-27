import { PostgresQuery } from '../../src';
import { BigqueryQuery } from '../../src/adapter/BigqueryQuery';
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
    // Calendar units have no length in seconds: the orchestrator adds the interval itself, by
    // calendar, the way the refresh key SQL does
    ['1 month', undefined],
    ['3 months', undefined],
    ['1 quarter', undefined],
    ['1 year', undefined],
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
    expect(description.updateWindow).toEqual(updateWindow);
    expect(description.updateWindowSeconds).toEqual(seconds);
    expect(description.invalidateKeyQueries[0][2]).toMatchObject({ updateWindow, updateWindowSeconds: seconds });
  });

  // BigQuery's TIMESTAMP_ADD has no calendar units and CURRENT_TIMESTAMP() can't be compared with
  // the DATETIME that DATETIME_ADD returns, so the window is added to a DATETIME and cast back.
  it.each([
    ['3 months', 'INTERVAL 3 MONTH'],
    ['1 quarter', 'INTERVAL 1 QUARTER'],
    ['1 year', 'INTERVAL 1 YEAR'],
  ])('compares like types in the BigQuery refresh key for update_window: %s', async (updateWindow, interval) => {
    const compilers = prepareYamlCompiler(schema(`
          every: 1 day
          incremental: true
          update_window: ${updateWindow}`));

    await compilers.compile();

    const query = compilers.createQuery(BigqueryQuery, {
      measures: ['orders.count'],
      timeDimensions: [{
        dimension: 'orders.created_at',
        granularity: 'day',
        dateRange: ['2024-01-01', '2024-01-31'],
      }],
      timezone: 'America/New_York',
    });

    const [description] = query.newPreAggregations().preAggregationsDescription();
    expect(description.invalidateKeyQueries[0][0]).toContain(
      `WHEN CURRENT_TIMESTAMP() < TIMESTAMP(DATETIME_ADD(DATETIME(TIMESTAMP(?)), ${interval})) THEN`
    );
  });

  it('still rejects calendar units in refresh_key.every', async () => {
    const compilers = prepareYamlCompiler(schema(`
          every: 1 month`));

    await compilers.compiler.compile().catch(() => undefined);
    expect(() => compilers.compiler.throwIfAnyErrors()).toThrow(/every = 1 month/);
  });
});
