import { getEnv } from '@cubejs-backend/shared';
import { prepareYamlCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

// Inline data:
//   id  level9  depth  amount  amount_3  amount_0  total  created_at
//   1   L1      3      10      100       1000      5      2024-01-05
//   2   L1      4      20      200       2000      5      2024-01-20
//   3   L2      2      30      300       3000      5      2024-02-05
//   4   L2      3      40      400       4000      5      2024-02-20
describe('QUERY_CONTEXT conditions in measure case', () => {
  jest.setTimeout(200000);

  const compilers = prepareYamlCompiler(`
cubes:
  - name: orders
    sql: >
      SELECT 1 AS id, 'L1' AS account_level9, 3 AS account_depth, 10 AS amount, 100 AS amount_3, 1000 AS amount_0, 5 AS total, '2024-01-05T00:00:00.000Z'::timestamptz AS created_at
      UNION ALL SELECT 2, 'L1', 4, 20, 200, 2000, 5, '2024-01-20T00:00:00.000Z'::timestamptz
      UNION ALL SELECT 3, 'L2', 2, 30, 300, 3000, 5, '2024-02-05T00:00:00.000Z'::timestamptz
      UNION ALL SELECT 4, 'L2', 3, 40, 400, 4000, 5, '2024-02-20T00:00:00.000Z'::timestamptz

    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: account_level9
        sql: account_level9
        type: string
      - name: account_depth
        sql: account_depth
        type: number
      - name: created_at
        sql: created_at
        type: time

    measures:
      - name: amount_raw
        sql: amount
        type: sum
      - name: amount_at_depth_3
        sql: amount_3
        type: sum
      - name: amount_at_depth_0
        sql: amount_0
        type: sum
      - name: total
        sql: total
        type: sum
      - name: amount
        multi_stage: true
        type: number
        case:
          when:
            - context: "{QUERY_CONTEXT.filtered(CUBE.account_depth) or QUERY_CONTEXT.filtered(CUBE.created_at)}"
              sql: "{amount_raw}"
            - context: "{QUERY_CONTEXT.single_value(CUBE.account_level9)}"
              sql: "{total}"
            - context: "{QUERY_CONTEXT.grouped(CUBE.account_level9) and not QUERY_CONTEXT.has_measure(CUBE.total)}"
              sql: "{amount_at_depth_3}"
          else:
            sql: "{amount_at_depth_0}"
    `);

  const cases: [string, Record<string, unknown>, Record<string, unknown>[]][] = [
    ['grouped by level9 alone reads the depth-3 amount', {
      measures: ['orders.amount'],
      dimensions: ['orders.account_level9'],
      order: [{ id: 'orders.account_level9' }],
    }, [
      { orders__account_level9: 'L1', orders__amount: '300' },
      { orders__account_level9: 'L2', orders__amount: '700' },
    ]],
    ['adding the total measure falls back to the depth-0 amount', {
      measures: ['orders.amount', 'orders.total'],
      dimensions: ['orders.account_level9'],
      order: [{ id: 'orders.account_level9' }],
    }, [
      { orders__account_level9: 'L1', orders__amount: '3000', orders__total: '10' },
      { orders__account_level9: 'L2', orders__amount: '7000', orders__total: '10' },
    ]],
    ['filtering by depth reads the raw amount over the filtered rows', {
      measures: ['orders.amount'],
      filters: [{ member: 'orders.account_depth', operator: 'gte', values: ['3'] }],
    }, [
      { orders__amount: '70' },
    ]],
    ['a date range without granularity counts as a filter, not a grouping', {
      measures: ['orders.amount'],
      timeDimensions: [{ dimension: 'orders.created_at', dateRange: ['2024-01-01', '2024-01-31'] }],
    }, [
      { orders__amount: '30' },
    ]],
    ['a single equals value on level9 takes the single_value branch', {
      measures: ['orders.amount'],
      dimensions: ['orders.account_level9'],
      filters: [{ member: 'orders.account_level9', operator: 'equals', values: ['L1'] }],
    }, [
      { orders__account_level9: 'L1', orders__amount: '10' },
    ]],
  ];

  for (const [name, query, expected] of cases) {
    if (getEnv('nativeSqlPlanner')) {
      it(name, async () => dbRunner.runQueryTest({ ...query, timezone: 'UTC' }, expected, compilers));
    } else {
      // The legacy planner refuses QUERY_CONTEXT conditions.
      test.skip(name, () => { expect(1).toBe(1); });
    }
  }
});
