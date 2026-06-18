import { getEnv } from '@cubejs-backend/shared';
import { prepareYamlCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

// Smoke tests for the multi-stage `filter:` directive going through the
// JS schema compiler end-to-end (YAML → Joi → transpiler → Tesseract → SQL).
// The Rust planner has the exhaustive coverage; this file just confirms the
// JS pipeline accepts the syntax and produces correct results for a few
// representative shapes.
//
// Inline data (5 rows):
//   id status     category    amount  created_at
//   1  completed  books       100     2024-01-10
//   2  completed  electronics 200     2024-01-15
//   3  pending    books        50     2024-02-12
//   4  pending    electronics  75     2024-02-18
//   5  cancelled  books        30     2024-03-10
//
//   by status:    completed=300, pending=125, cancelled=30
//   by category:  books=180, electronics=275
//   status×cat:   completed×books=100, completed×electronics=200,
//                 pending×books=50, pending×electronics=75,
//                 cancelled×books=30
describe('Multi-Stage filter directive', () => {
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

      # include: forces status=completed at leaf, regardless of query filters.
      - name: amount_only_completed
        sql: "{CUBE.total_amount}"
        type: number
        multi_stage: true
        filter:
          include:
            - member: orders.status
              operator: equals
              values: [completed]

      # exclude: drops a query-level filter on status from the leaf state.
      - name: amount_exclude_status
        sql: "{CUBE.total_amount}"
        type: number
        multi_stage: true
        filter:
          exclude:
            - orders.status

      # Chain: mode: fixed inside a chain — diverges from relative.
      - name: x_books_relative
        sql: "{CUBE.total_amount}"
        type: number
        multi_stage: true
        filter:
          mode: relative
          include:
            - member: orders.category
              operator: equals
              values: [books]

      - name: x_books_fixed
        sql: "{CUBE.total_amount}"
        type: number
        multi_stage: true
        filter:
          mode: fixed
          include:
            - member: orders.category
              operator: equals
              values: [books]

      - name: t_chain_relative
        sql: "{CUBE.x_books_relative}"
        type: number
        multi_stage: true
        filter:
          include:
            - member: orders.status
              operator: equals
              values: [completed]

      - name: t_chain_fixed
        sql: "{CUBE.x_books_fixed}"
        type: number
        multi_stage: true
        filter:
          include:
            - member: orders.status
              operator: equals
              values: [completed]
    `);

  if (getEnv('nativeSqlPlanner')) {
    it('include: simple dim filter', async () => dbRunner.runQueryTest({
      measures: ['orders.total_amount', 'orders.amount_only_completed'],
      dimensions: ['orders.category'],
      order: [{ id: 'orders.category' }],
      timezone: 'UTC',
    }, [
      { orders__category: 'books', orders__total_amount: '180', orders__amount_only_completed: '100' },
      { orders__category: 'electronics', orders__total_amount: '275', orders__amount_only_completed: '200' },
    ], { joinGraph, cubeEvaluator, compiler }));

    it('exclude: drops query filter at leaf', async () => dbRunner.runQueryTest({
      measures: ['orders.total_amount', 'orders.amount_exclude_status'],
      dimensions: ['orders.category'],
      filters: [
        { member: 'orders.status', operator: 'equals', values: ['completed'] },
      ],
      order: [{ id: 'orders.category' }],
      timezone: 'UTC',
    }, [
      { orders__category: 'books', orders__total_amount: '100', orders__amount_exclude_status: '180' },
      { orders__category: 'electronics', orders__total_amount: '200', orders__amount_exclude_status: '275' },
    ], { joinGraph, cubeEvaluator, compiler }));

    it('mode: fixed in chain diverges from relative', async () => dbRunner.runQueryTest({
      measures: ['orders.t_chain_relative', 'orders.t_chain_fixed'],
      dimensions: ['orders.category'],
      order: [{ id: 'orders.category' }],
      timezone: 'UTC',
    }, [
      { orders__category: 'books', orders__t_chain_relative: '100', orders__t_chain_fixed: '180' },
    ], { joinGraph, cubeEvaluator, compiler }));
  } else {
    // These tests rely on Tesseract; v1 planner does not implement the directive.
    test.skip('include: simple dim filter', () => { expect(1).toBe(1); });
    test.skip('exclude: drops query filter at leaf', () => { expect(1).toBe(1); });
    test.skip('mode: fixed in chain diverges from relative', () => { expect(1).toBe(1); });
  }
});
