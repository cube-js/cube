import { getEnv } from '@cubejs-backend/shared';
import { prepareYamlCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

// CORE-549: a `rank` multi-stage measure whose `order_by` template references a
// member (`num_parcels`) that is NOT exposed by the view fails to compile: the
// order_by template is resolved against the view's cube context instead of the
// measure's owning cube, so `num_parcels` cannot be found.
//
// The ticket reported this as a JS crash (`Cannot read properties of undefined
// (reading '_objectWithResolvedProperties')` in BaseQuery.js). On current master
// order_by template compilation happens in the Tesseract planner, so it now
// surfaces as `Cannot resolve: num_parcels` — same root cause, different surface.
//
// The base-cube control (querying `orders.*` directly) compiles and returns the
// ranks; the view query (`orders_view.*`, with `num_parcels` deliberately not in
// `includes`) reproduces the failure.
//
// Inline data (per-day sums so the two days rank distinctly):
//   id parcels created_at
//   1  10      2024-01-01   -> day 2024-01-01 total = 30
//   2  20      2024-01-01
//   3  30      2024-01-02   -> day 2024-01-02 total = 80
//   4  50      2024-01-02
//
// volume_by_day_rank: reduce_by created_at removes the day from the window
// partition, so the rank is global over days ordered by num_parcels desc:
//   2024-01-02 (80) -> 1, 2024-01-01 (30) -> 2.
describe('Multi-Stage rank order_by through a view (CORE-549)', () => {
  jest.setTimeout(200000);

  const { compiler, joinGraph, cubeEvaluator } = prepareYamlCompiler(`
cubes:
  - name: orders
    sql: >
      SELECT 1 AS id, 10 AS parcels, '2024-01-01T00:00:00.000Z'::timestamptz AS created_at
      union all
      SELECT 2 AS id, 20 AS parcels, '2024-01-01T00:00:00.000Z'::timestamptz AS created_at
      union all
      SELECT 3 AS id, 30 AS parcels, '2024-01-02T00:00:00.000Z'::timestamptz AS created_at
      union all
      SELECT 4 AS id, 50 AS parcels, '2024-01-02T00:00:00.000Z'::timestamptz AS created_at

    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true

      - name: created_at
        sql: created_at
        type: time

    measures:
      - name: num_parcels
        type: sum
        sql: parcels

      - name: volume_by_day_rank
        type: rank
        multi_stage: true
        reduce_by:
          - created_at
        order_by:
          - sql: "{num_parcels}"
            dir: desc

views:
  - name: orders_view
    cubes:
      - join_path: orders
        includes:
          - created_at
          - volume_by_day_rank
          # num_parcels deliberately NOT exposed
    `);

  if (getEnv('nativeSqlPlanner')) {
    // Control: querying the base cube compiles cleanly (num_parcels resolves in
    // the owning cube's context).
    it('base cube: rank order_by references a non-selected member', async () => dbRunner.runQueryTest({
      measures: ['orders.volume_by_day_rank'],
      timeDimensions: [
        { dimension: 'orders.created_at', granularity: 'day' }
      ],
      order: [{ id: 'orders.created_at' }],
      timezone: 'UTC',
    }, [
      { orders__created_at_day: '2024-01-01T00:00:00.000Z', orders__volume_by_day_rank: '2' },
      { orders__created_at_day: '2024-01-02T00:00:00.000Z', orders__volume_by_day_rank: '1' },
    ], { joinGraph, cubeEvaluator, compiler }));

    // Repro: the same measure through a view that does not expose num_parcels.
    it('view: rank order_by references a member not exposed by the view', async () => dbRunner.runQueryTest({
      measures: ['orders_view.volume_by_day_rank'],
      timeDimensions: [
        { dimension: 'orders_view.created_at', granularity: 'day' }
      ],
      order: [{ id: 'orders_view.created_at' }],
      timezone: 'UTC',
    }, [
      { orders_view__created_at_day: '2024-01-01T00:00:00.000Z', orders_view__volume_by_day_rank: '2' },
      { orders_view__created_at_day: '2024-01-02T00:00:00.000Z', orders_view__volume_by_day_rank: '1' },
    ], { joinGraph, cubeEvaluator, compiler }));
  } else {
    // Multi-stage rank is Tesseract-only.
    test.skip('base cube: rank order_by references a non-selected member', () => { expect(1).toBe(1); });
    test.skip('view: rank order_by references a member not exposed by the view', () => { expect(1).toBe(1); });
  }
});
