import { getEnv } from '@cubejs-backend/shared';
import { prepareYamlCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

// Inline data:
//   id  level9  depth  amount  amount_3  amount_0  total
//   1   L1      3      10      100       1000      5
//   2   L1      4      20      200       2000      5
//   3   L2      2      30      300       3000      5
//   4   L2      3      40      400       4000      5
describe('QUERY_CONTEXT conditions in measure case', () => {
  jest.setTimeout(200000);

  const compilers = prepareYamlCompiler(`
cubes:
  - name: orders
    sql: >
      SELECT 1 AS id, 'L1' AS account_level9, 3 AS account_depth, 10 AS amount, 100 AS amount_3, 1000 AS amount_0, 5 AS total
      UNION ALL SELECT 2, 'L1', 4, 20, 200, 2000, 5
      UNION ALL SELECT 3, 'L2', 2, 30, 300, 3000, 5
      UNION ALL SELECT 4, 'L2', 3, 40, 400, 4000, 5

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
            - context: "{QUERY_CONTEXT.filtered(CUBE.account_depth)}"
              sql: "{amount_raw}"
            - context: "{QUERY_CONTEXT.grouped(CUBE.account_level9) and not QUERY_CONTEXT.has_measure(CUBE.total)}"
              sql: "{amount_at_depth_3}"
          else:
            sql: "{amount_at_depth_0}"
    `);

  if (getEnv('nativeSqlPlanner')) {
    it('grouped by level9 alone reads the depth-3 amount', async () => dbRunner.runQueryTest({
      measures: ['orders.amount'],
      dimensions: ['orders.account_level9'],
      order: [{ id: 'orders.account_level9' }],
      timezone: 'UTC',
    }, [
      { orders__account_level9: 'L1', orders__amount: '300' },
      { orders__account_level9: 'L2', orders__amount: '700' },
    ], compilers));

    it('adding the total measure falls back to the depth-0 amount', async () => dbRunner.runQueryTest({
      measures: ['orders.amount', 'orders.total'],
      dimensions: ['orders.account_level9'],
      order: [{ id: 'orders.account_level9' }],
      timezone: 'UTC',
    }, [
      { orders__account_level9: 'L1', orders__amount: '3000', orders__total: '10' },
      { orders__account_level9: 'L2', orders__amount: '7000', orders__total: '10' },
    ], compilers));

    it('filtering by depth reads the raw amount over the filtered rows', async () => dbRunner.runQueryTest({
      measures: ['orders.amount'],
      filters: [{ member: 'orders.account_depth', operator: 'gte', values: ['3'] }],
      timezone: 'UTC',
    }, [
      { orders__amount: '70' },
    ], compilers));
  } else {
    // The legacy planner refuses QUERY_CONTEXT conditions.
    test.skip('grouped by level9 alone reads the depth-3 amount', () => { expect(1).toBe(1); });
    test.skip('adding the total measure falls back to the depth-0 amount', () => { expect(1).toBe(1); });
    test.skip('filtering by depth reads the raw amount over the filtered rows', () => { expect(1).toBe(1); });
  }
});
