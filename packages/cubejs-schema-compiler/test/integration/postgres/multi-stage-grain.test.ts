import { getEnv } from '@cubejs-backend/shared';
import { prepareYamlCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

// Smoke tests for the multi-stage `grain:` directive going through the
// JS schema compiler end-to-end (YAML → Joi → transpiler → Tesseract → SQL).
// The Rust planner has the exhaustive coverage; this file just confirms the
// JS pipeline accepts each grain shape and produces correct results.
//
// Inline data (6 rows — id=6 added so (completed, books) has two rows,
// which lets the 2-element keep_only test distinguish from total_amount):
//   id status     category    amount  created_at
//   1  completed  books       100     2024-01-10
//   2  completed  electronics 200     2024-01-15
//   3  pending    books        50     2024-02-12
//   4  pending    electronics  75     2024-02-18
//   5  cancelled  books        30     2024-03-10
//   6  completed  books        70     2024-01-20
//
//   by status:           completed=370, pending=125, cancelled=30
//   by category:         books=250, electronics=275
//   by (status,category): (completed,books)=170, (completed,electronics)=200,
//                         (pending,books)=50, (pending,electronics)=75,
//                         (cancelled,books)=30
//   total:               525
describe('Multi-Stage grain directive', () => {
  jest.setTimeout(200000);

  const { compiler, joinGraph, cubeEvaluator } = prepareYamlCompiler(`
cubes:
  - name: orders
    sql: >
      SELECT 1 as ID, 'completed' as STATUS, 'books' as CATEGORY, 100 as AMOUNT, '2024-01-10T00:00:00.000Z'::timestamptz as CREATED_AT
      union all
      SELECT 2 as ID, 'completed' as STATUS, 'electronics' as CATEGORY, 200 as AMOUNT, '2024-01-15T00:00:00.000Z'::timestamptz as CREATED_AT
      union all
      SELECT 3 as ID, 'pending' as STATUS, 'books' as CATEGORY, 50 as AMOUNT, '2024-02-12T00:00:00.000Z'::timestamptz as CREATED_AT
      union all
      SELECT 4 as ID, 'pending' as STATUS, 'electronics' as CATEGORY, 75 as AMOUNT, '2024-02-18T00:00:00.000Z'::timestamptz as CREATED_AT
      union all
      SELECT 5 as ID, 'cancelled' as STATUS, 'books' as CATEGORY, 30 as AMOUNT, '2024-03-10T00:00:00.000Z'::timestamptz as CREATED_AT
      union all
      SELECT 6 as ID, 'completed' as STATUS, 'books' as CATEGORY, 70 as AMOUNT, '2024-01-20T00:00:00.000Z'::timestamptz as CREATED_AT

    dimensions:
      - name: id
        sql: ID
        type: number
        primary_key: true

      - name: status
        sql: STATUS
        type: string

      - name: category
        sql: CATEGORY
        type: string

      - name: created_at
        sql: CREATED_AT
        type: time

    measures:
      - name: total_amount
        sql: AMOUNT
        type: sum

      # ── single-element variants ────────────────────────────────────
      - name: amount_grain_exclude_status
        sql: "{CUBE.total_amount}"
        type: sum
        multi_stage: true
        grain:
          exclude:
            - orders.status

      - name: amount_grain_keep_only_status
        sql: "{CUBE.total_amount}"
        type: sum
        multi_stage: true
        grain:
          keep_only:
            - orders.status

      - name: amount_grain_include_status
        sql: "{CUBE.total_amount}"
        type: sum
        multi_stage: true
        grain:
          include:
            - orders.status

      # ── two-element arrays ────────────────────────────────────────
      - name: amount_grain_exclude_status_id
        sql: "{CUBE.total_amount}"
        type: sum
        multi_stage: true
        grain:
          exclude:
            - orders.status
            - orders.id

      - name: amount_grain_keep_only_status_category
        sql: "{CUBE.total_amount}"
        type: sum
        multi_stage: true
        grain:
          keep_only:
            - orders.status
            - orders.category

      - name: amount_grain_include_status_id
        sql: "{CUBE.total_amount}"
        type: sum
        multi_stage: true
        grain:
          include:
            - orders.status
            - orders.id

      # ── keep_only + include combination ──────────────────────────
      # keep_only shrinks the inherited partition to [status]; include
      # then extends the leaf grain with [id]. Outer re-aggregates over
      # id back to the query grain — equivalent to "per-status total
      # broadcast across categories" (the Rust planner pattern
      # mirrored from total_by_customer_reduce_category).
      - name: amount_grain_keep_status_include_id
        sql: "{CUBE.total_amount}"
        type: sum
        multi_stage: true
        grain:
          keep_only:
            - orders.status
          include:
            - orders.id
    `);

  if (getEnv('nativeSqlPlanner')) {
    // ── single-element variants ──────────────────────────────────
    it('exclude: drops a dim from the partition', async () => dbRunner.runQueryTest({
      measures: ['orders.total_amount', 'orders.amount_grain_exclude_status'],
      dimensions: ['orders.status', 'orders.category'],
      order: [{ id: 'orders.status' }, { id: 'orders.category' }],
      timezone: 'UTC',
    }, [
      // exclude_status removes status from the window's PARTITION BY → each
      // row gets the per-category total regardless of its status.
      { orders__status: 'cancelled', orders__category: 'books', orders__total_amount: '30', orders__amount_grain_exclude_status: '250' },
      { orders__status: 'completed', orders__category: 'books', orders__total_amount: '170', orders__amount_grain_exclude_status: '250' },
      { orders__status: 'completed', orders__category: 'electronics', orders__total_amount: '200', orders__amount_grain_exclude_status: '275' },
      { orders__status: 'pending', orders__category: 'books', orders__total_amount: '50', orders__amount_grain_exclude_status: '250' },
      { orders__status: 'pending', orders__category: 'electronics', orders__total_amount: '75', orders__amount_grain_exclude_status: '275' },
    ], { joinGraph, cubeEvaluator, compiler }));

    it('keep_only: shrinks the partition to the listed dim', async () => dbRunner.runQueryTest({
      measures: ['orders.total_amount', 'orders.amount_grain_keep_only_status'],
      dimensions: ['orders.status', 'orders.category'],
      order: [{ id: 'orders.status' }, { id: 'orders.category' }],
      timezone: 'UTC',
    }, [
      // keep_only [status] reduces the partition to status only → each row
      // gets the per-status total regardless of its category.
      { orders__status: 'cancelled', orders__category: 'books', orders__total_amount: '30', orders__amount_grain_keep_only_status: '30' },
      { orders__status: 'completed', orders__category: 'books', orders__total_amount: '170', orders__amount_grain_keep_only_status: '370' },
      { orders__status: 'completed', orders__category: 'electronics', orders__total_amount: '200', orders__amount_grain_keep_only_status: '370' },
      { orders__status: 'pending', orders__category: 'books', orders__total_amount: '50', orders__amount_grain_keep_only_status: '125' },
      { orders__status: 'pending', orders__category: 'electronics', orders__total_amount: '75', orders__amount_grain_keep_only_status: '125' },
    ], { joinGraph, cubeEvaluator, compiler }));

    it('keep_only: dim absent from query collapses to grand total', async () => dbRunner.runQueryTest({
      measures: ['orders.total_amount', 'orders.amount_grain_keep_only_status'],
      dimensions: ['orders.category'],
      order: [{ id: 'orders.category' }],
      timezone: 'UTC',
    }, [
      // status not in the query → keep_only intersection with the query dims
      // is empty → the measure collapses to a grand total (525) per row.
      { orders__category: 'books', orders__total_amount: '250', orders__amount_grain_keep_only_status: '525' },
      { orders__category: 'electronics', orders__total_amount: '275', orders__amount_grain_keep_only_status: '525' },
    ], { joinGraph, cubeEvaluator, compiler }));

    it('include: extends the leaf grain, outer re-aggregates', async () => dbRunner.runQueryTest({
      measures: ['orders.total_amount', 'orders.amount_grain_include_status'],
      dimensions: ['orders.category'],
      order: [{ id: 'orders.category' }],
      timezone: 'UTC',
    }, [
      // Inner CTE groups by (category, status); outer SUMs back over status.
      // SUM is associative so the per-category result equals total_amount —
      // the test verifies pipeline acceptance, not a math divergence.
      { orders__category: 'books', orders__total_amount: '250', orders__amount_grain_include_status: '250' },
      { orders__category: 'electronics', orders__total_amount: '275', orders__amount_grain_include_status: '275' },
    ], { joinGraph, cubeEvaluator, compiler }));

    // ── two-element arrays ───────────────────────────────────────
    it('exclude: two-element array drops both dims', async () => dbRunner.runQueryTest({
      measures: ['orders.total_amount', 'orders.amount_grain_exclude_status_id'],
      dimensions: ['orders.status', 'orders.category', 'orders.id'],
      order: [{ id: 'orders.status' }, { id: 'orders.category' }, { id: 'orders.id' }],
      timezone: 'UTC',
    }, [
      // exclude [status, id] removes both from the partition → partition is
      // [category] alone → each row gets the per-category total.
      { orders__status: 'cancelled', orders__category: 'books', orders__id: 5, orders__total_amount: '30', orders__amount_grain_exclude_status_id: '250' },
      { orders__status: 'completed', orders__category: 'books', orders__id: 1, orders__total_amount: '100', orders__amount_grain_exclude_status_id: '250' },
      { orders__status: 'completed', orders__category: 'books', orders__id: 6, orders__total_amount: '70', orders__amount_grain_exclude_status_id: '250' },
      { orders__status: 'completed', orders__category: 'electronics', orders__id: 2, orders__total_amount: '200', orders__amount_grain_exclude_status_id: '275' },
      { orders__status: 'pending', orders__category: 'books', orders__id: 3, orders__total_amount: '50', orders__amount_grain_exclude_status_id: '250' },
      { orders__status: 'pending', orders__category: 'electronics', orders__id: 4, orders__total_amount: '75', orders__amount_grain_exclude_status_id: '275' },
    ], { joinGraph, cubeEvaluator, compiler }));

    it('keep_only: two-element array narrows to the (status, category) cell', async () => dbRunner.runQueryTest({
      measures: ['orders.total_amount', 'orders.amount_grain_keep_only_status_category'],
      dimensions: ['orders.status', 'orders.category', 'orders.id'],
      order: [{ id: 'orders.status' }, { id: 'orders.category' }, { id: 'orders.id' }],
      timezone: 'UTC',
    }, [
      // keep_only [status, category] reduces the partition to that pair →
      // each row gets the per-(status,category) total. id rows within the
      // same (status, category) share the value (e.g. id=1 and id=6 both
      // get 170 for (completed, books)).
      { orders__status: 'cancelled', orders__category: 'books', orders__id: 5, orders__total_amount: '30', orders__amount_grain_keep_only_status_category: '30' },
      { orders__status: 'completed', orders__category: 'books', orders__id: 1, orders__total_amount: '100', orders__amount_grain_keep_only_status_category: '170' },
      { orders__status: 'completed', orders__category: 'books', orders__id: 6, orders__total_amount: '70', orders__amount_grain_keep_only_status_category: '170' },
      { orders__status: 'completed', orders__category: 'electronics', orders__id: 2, orders__total_amount: '200', orders__amount_grain_keep_only_status_category: '200' },
      { orders__status: 'pending', orders__category: 'books', orders__id: 3, orders__total_amount: '50', orders__amount_grain_keep_only_status_category: '50' },
      { orders__status: 'pending', orders__category: 'electronics', orders__id: 4, orders__total_amount: '75', orders__amount_grain_keep_only_status_category: '75' },
    ], { joinGraph, cubeEvaluator, compiler }));

    it('include: two-element array extends the leaf grain', async () => dbRunner.runQueryTest({
      measures: ['orders.total_amount', 'orders.amount_grain_include_status_id'],
      dimensions: ['orders.category'],
      order: [{ id: 'orders.category' }],
      timezone: 'UTC',
    }, [
      // Leaf grain extends to (category, status, id). Outer SUMs back to
      // the per-category total — same numbers as total_amount, the test
      // verifies that a two-element include list threads through.
      { orders__category: 'books', orders__total_amount: '250', orders__amount_grain_include_status_id: '250' },
      { orders__category: 'electronics', orders__total_amount: '275', orders__amount_grain_include_status_id: '275' },
    ], { joinGraph, cubeEvaluator, compiler }));

    // ── keep_only + include combination ──────────────────────────
    it('keep_only + include: keep narrows, include extends', async () => dbRunner.runQueryTest({
      measures: ['orders.total_amount', 'orders.amount_grain_keep_status_include_id'],
      dimensions: ['orders.status', 'orders.category'],
      order: [{ id: 'orders.status' }, { id: 'orders.category' }],
      timezone: 'UTC',
    }, [
      // keep_only [status] narrows parent grain to [status]; include [id]
      // adds id to the leaf. Outer re-aggregates by (status, category) →
      // per-status total broadcast across categories (completed=370 to both
      // books and electronics; pending=125 to both; cancelled=30 to books).
      { orders__status: 'cancelled', orders__category: 'books', orders__total_amount: '30', orders__amount_grain_keep_status_include_id: '30' },
      { orders__status: 'completed', orders__category: 'books', orders__total_amount: '170', orders__amount_grain_keep_status_include_id: '370' },
      { orders__status: 'completed', orders__category: 'electronics', orders__total_amount: '200', orders__amount_grain_keep_status_include_id: '370' },
      { orders__status: 'pending', orders__category: 'books', orders__total_amount: '50', orders__amount_grain_keep_status_include_id: '125' },
      { orders__status: 'pending', orders__category: 'electronics', orders__total_amount: '75', orders__amount_grain_keep_status_include_id: '125' },
    ], { joinGraph, cubeEvaluator, compiler }));
  } else {
    // These tests rely on Tesseract; v1 planner does not implement the directive.
    test.skip('exclude: drops a dim from the partition', () => { expect(1).toBe(1); });
    test.skip('keep_only: shrinks the partition to the listed dim', () => { expect(1).toBe(1); });
    test.skip('keep_only: dim absent from query collapses to grand total', () => { expect(1).toBe(1); });
    test.skip('include: extends the leaf grain, outer re-aggregates', () => { expect(1).toBe(1); });
    test.skip('exclude: two-element array drops both dims', () => { expect(1).toBe(1); });
    test.skip('keep_only: two-element array narrows to the (status, category) cell', () => { expect(1).toBe(1); });
    test.skip('include: two-element array extends the leaf grain', () => { expect(1).toBe(1); });
    test.skip('keep_only + include: keep narrows, include extends', () => { expect(1).toBe(1); });
  }
});
