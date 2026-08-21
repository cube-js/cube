import { getEnv } from '@cubejs-backend/shared';
import { PostgresQuery } from '../../../src/adapter/PostgresQuery';
import { prepareYamlCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

// Regression for GitHub #11536:
// When a `multi_stage` time_shift measure is served from a rollup, the matched
// partition range must be widened backwards by the shift interval so the shifted
// leaf can find its rows. `dimensions_shifts` is keyed by reference-chain-resolved
// names, but the date-range lookup used the unresolved filter member name. A view
// member is a reference to the underlying cube member, so the lookup missed for
// view-qualified queries, the range stayed unwidened, and the shifted measure
// silently returned NULL for every row while the identical cube-qualified query
// returned correct values.
describe('Pre-aggregations + multi-stage time_shift over a view', () => {
  jest.setTimeout(200000);

  const { compiler, joinGraph, cubeEvaluator } = prepareYamlCompiler(`
cubes:
  - name: orders
    sql: >
      SELECT * FROM (
        SELECT 1 as id, '2017-01-01T00:00:00.000Z'::timestamptz as created_at, 100 as amount
        union all
        SELECT 2 as id, '2017-01-08T00:00:00.000Z'::timestamptz as created_at, 300 as amount
      ) AS t

    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true

      - name: created_at
        sql: created_at
        type: time

    measures:
      - name: amount
        sql: amount
        type: sum

      - name: amount_prior_week
        sql: "{amount}"
        multi_stage: true
        type: number
        time_shift:
          - time_dimension: created_at
            interval: 7 day
            type: prior

    pre_aggregations:
      - name: daily_rollup
        type: rollup
        measures:
          - amount
        time_dimension: created_at
        granularity: day
        partition_granularity: week

views:
  - name: orders_view
    cubes:
      - join_path: orders
        includes:
          - created_at
          - amount
          - amount_prior_week
    `);

  // The query range covers exactly the shift interval, so it does not itself
  // contain the prior-week rows — widening is the only way the shifted leaf
  // can reach them.
  const timeDimensions = (dimension: string) => [{
    dimension,
    granularity: 'day',
    dateRange: ['2017-01-08', '2017-01-08'],
  }];

  if (getEnv('nativeSqlPlanner')) {
    it('widens the matched partition range for a view-qualified query', () => compiler.compile().then(() => {
      const viewQuery = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: ['orders_view.amount', 'orders_view.amount_prior_week'],
        timeDimensions: timeDimensions('orders_view.created_at'),
        timezone: 'UTC',
        preAggregationsSchema: '',
        cubestoreSupportMultistage: true,
      });

      const cubeQuery = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: ['orders.amount', 'orders.amount_prior_week'],
        timeDimensions: timeDimensions('orders.created_at'),
        timezone: 'UTC',
        preAggregationsSchema: '',
        cubestoreSupportMultistage: true,
      });

      const viewDescription: any = viewQuery.preAggregations?.preAggregationsDescription();
      const cubeDescription: any = cubeQuery.preAggregations?.preAggregationsDescription();

      // The shifted usage must carry the same widened range in both cases.
      // Before the fix the view range was left at ['2017-01-08', '2017-01-08'].
      expect(viewDescription[0].matchedTimeDimensionDateRange)
        .toEqual(cubeDescription[0].matchedTimeDimensionDateRange);
    }));

    it('returns the same prior-week value through the view as through the cube', () => compiler.compile().then(() => {
      const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: ['orders_view.amount', 'orders_view.amount_prior_week'],
        timeDimensions: timeDimensions('orders_view.created_at'),
        timezone: 'UTC',
        order: [{ id: 'orders_view.created_at' }],
        preAggregationsSchema: '',
        cubestoreSupportMultistage: true,
      });

      // Before the fix orders_view__amount_prior_week came back as null.
      return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
        expect(res).toEqual([
          {
            orders_view__created_at_day: '2017-01-08T00:00:00.000Z',
            orders_view__amount: '300',
            orders_view__amount_prior_week: '100',
          },
        ]);
      });
    }));
  } else {
    // Tesseract-only: the pre-aggregation optimizer under test is the native planner's.
    test.skip('widens the matched partition range for a view-qualified query', () => { expect(1).toBe(1); });
    test.skip('returns the same prior-week value through the view as through the cube', () => { expect(1).toBe(1); });
  }
});
