import { getEnv } from '@cubejs-backend/shared';
import { PostgresQuery } from '../../../src/adapter/PostgresQuery';
import { prepareJsCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

// https://github.com/cube-js/cube/issues/12088
//
// A `measureFilter` on a multi-stage measure placed inside an `or` group:
// when planning the multi-stage leg, Tesseract removes the measure's own
// entry from the group and keeps the rest. An emptied group renders as
// `... AND )` (invalid SQL), and a group with another entry left narrows the
// leg's WHERE to that entry's predicate.
//
// Inline data:
//   id status  amount created_at
//   1  paid    100    2025-01-15
//   2  unpaid   40    2025-02-10
//   3  paid     60    2025-03-05
describe('Multi-stage measureFilter inside or group', () => {
  jest.setTimeout(200000);

  const { compiler, joinGraph, cubeEvaluator } = prepareJsCompiler(`
cube('orders', {
  sql: \`
    SELECT 1 AS id, 'paid' AS status, 100.0 AS amount, '2025-01-15T00:00:00.000Z'::timestamptz AS created_at
    UNION ALL
    SELECT 2 AS id, 'unpaid' AS status, 40.0 AS amount, '2025-02-10T00:00:00.000Z'::timestamptz AS created_at
    UNION ALL
    SELECT 3 AS id, 'paid' AS status, 60.0 AS amount, '2025-03-05T00:00:00.000Z'::timestamptz AS created_at
  \`,
  dimensions: {
    id: { sql: 'id', type: 'number', primary_key: true },
    created_at: { sql: 'created_at', type: 'time' },
  },
  measures: {
    amount: { sql: 'amount', type: 'sum' },
    paid_amount: { sql: 'amount', type: 'sum', filters: [{ sql: \`\${CUBE}.status = 'paid'\` }] },
    unpaid_amount: { sql: 'amount', type: 'sum', filters: [{ sql: \`\${CUBE}.status = 'unpaid'\` }] },
    paid_amount_ytd: { sql: 'amount', type: 'sum', filters: [{ sql: \`\${CUBE}.status = 'paid'\` }],
      rolling_window: { type: 'to_date', granularity: 'year' } },
    amount_ytd: { sql: 'amount', type: 'sum', rolling_window: { type: 'to_date', granularity: 'year' } },
    paid_ratio_ytd: { multi_stage: true, sql: \`\${paid_amount_ytd} / NULLIF(\${amount_ytd}, 0)\`, type: 'number' },
    paid_amount_avg_12m: { multi_stage: true, sql: \`\${paid_amount}\`, type: 'avg',
      rolling_window: { trailing: '12 month', offset: 'end' }, add_group_by: [created_at.month] },
  },
});
  `);

  const timeDimensions = [{ dimension: 'orders.created_at', dateRange: ['2025-01-01', '2025-12-31'] }];

  async function runQuery(q: any) {
    await compiler.compile();
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, { ...q, timezone: 'UTC' });
    const sqlAndParams = query.buildSqlAndParams();
    return dbRunner.testQuery(sqlAndParams);
  }

  if (getEnv('nativeSqlPlanner')) {
    it('control: measureFilter without or wrapper', async () => {
      const res = await runQuery({
        measures: ['orders.paid_ratio_ytd'],
        timeDimensions,
        filters: [{ member: 'orders.paid_ratio_ytd', operator: 'measureFilter' }],
      });
      expect(Number(res[0].orders__paid_ratio_ytd)).toBeCloseTo(0.8);
    });

    it('case A: or group with only the multi-stage measure produces valid SQL', async () => {
      const res = await runQuery({
        measures: ['orders.paid_ratio_ytd'],
        timeDimensions,
        filters: [{ or: [{ member: 'orders.paid_ratio_ytd', operator: 'measureFilter' }] }],
      });
      expect(Number(res[0].orders__paid_ratio_ytd)).toBeCloseTo(0.8);
    });

    it('control: no filter', async () => {
      const res = await runQuery({
        measures: ['orders.paid_amount_avg_12m', 'orders.unpaid_amount'],
        timeDimensions,
      });
      expect(Number(res[0].orders__paid_amount_avg_12m)).toBeCloseTo(80);
      expect(Number(res[0].orders__unpaid_amount)).toBeCloseTo(40);
    });

    it('case B: or group with another measure does not narrow the multi-stage leg', async () => {
      const res = await runQuery({
        measures: ['orders.paid_amount_avg_12m', 'orders.unpaid_amount'],
        timeDimensions,
        filters: [{
          or: [
            { member: 'orders.paid_amount_avg_12m', operator: 'measureFilter' },
            { member: 'orders.unpaid_amount', operator: 'measureFilter' },
          ]
        }],
      });
      expect(Number(res[0].orders__paid_amount_avg_12m)).toBeCloseTo(80);
      expect(Number(res[0].orders__unpaid_amount)).toBeCloseTo(40);
    });
  } else {
    test.skip('Tesseract only', () => { expect(1).toBe(1); });
  }
});
