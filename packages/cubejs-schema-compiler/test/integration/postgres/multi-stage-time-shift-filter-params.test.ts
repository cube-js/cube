import { getEnv } from '@cubejs-backend/shared';
import { prepareYamlCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

// Regression for CORE-543 (GitHub #11030):
// A multi_stage time_shift measure over a cube whose `sql` uses FILTER_PARAMS
// must render the prior-period CTE with the FILTER_PARAMS column shifted by the
// same interval as the time-shift predicate. Otherwise the current-period
// bounds on the un-shifted column contradict the shifted predicate, the
// prior-period CTE is always empty, and the YoY measure collapses to null.
describe('Multi-Stage time_shift + FILTER_PARAMS', () => {
  jest.setTimeout(200000);

  const { compiler, joinGraph, cubeEvaluator } = prepareYamlCompiler(`
cubes:
  - name: orders
    sql: >
      SELECT * FROM (
        SELECT 1 as id, '2024-03-14T00:00:00.000Z'::timestamptz as created_at, 100 as amount
        union all
        SELECT 2 as id, '2025-03-14T00:00:00.000Z'::timestamptz as created_at, 300 as amount
      ) AS t
      WHERE {FILTER_PARAMS.orders.date.filter('created_at')}

    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true

      - name: date
        sql: created_at
        type: time

    measures:
      - name: revenue
        sql: amount
        type: sum

      - name: revenue_1_y_ago
        sql: "{revenue}"
        multi_stage: true
        type: number
        time_shift:
          - time_dimension: date
            interval: 1 year
            type: prior
    `);

  if (getEnv('nativeSqlPlanner')) {
    it('prior-year time_shift measure is not emptied by FILTER_PARAMS', async () => dbRunner.runQueryTest({
      measures: ['orders.revenue', 'orders.revenue_1_y_ago'],
      timeDimensions: [
        {
          dimension: 'orders.date',
          granularity: 'year',
          dateRange: ['2025-01-01', '2025-12-31'],
        }
      ],
      timezone: 'UTC',
    }, [
      {
        orders__date_year: '2025-01-01T00:00:00.000Z',
        orders__revenue: '300',
        orders__revenue_1_y_ago: '100',
      },
    ], { compiler, joinGraph, cubeEvaluator }));
  }
});
