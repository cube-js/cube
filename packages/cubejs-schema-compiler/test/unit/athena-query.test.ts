/* eslint-disable no-restricted-syntax */
import { AthenaQuery } from '../../src/adapter/AthenaQuery';
import { prepareJsCompiler } from './PrepareCompiler';

describe('AthenaQuery', () => {
  const { compiler, joinGraph, cubeEvaluator } = prepareJsCompiler(`
    cube('orders', {
      sql: \`
        SELECT *
        FROM public.orders
      \`,
      dimensions: {
        id: {
          sql: \`id\`,
          type: 'number',
          primaryKey: true
        },
        created_at: {
          sql: \`created_at\`,
          type: 'time'
        }
      },
      measures: {
        count: {
          type: 'count',
        }
      }
    });
  `);

  it('convertTz uses manual offset calculation (inherits from PrestodbQuery)', async () => {
    await compiler.compile();

    const query = new AthenaQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: ['orders.count'],
      timeDimensions: [{
        dimension: 'orders.created_at',
        granularity: 'month',
        dateRange: ['2026-01-01', '2026-12-31'],
      }],
      timezone: 'Asia/Kolkata',
    });

    const queryAndParams = query.buildSqlAndParams();
    const sql = queryAndParams[0];

    expect(sql).toContain('timezone_hour');
    expect(sql).toContain('timezone_minute');
    expect(sql).not.toMatch(
      /CAST\(\(.*AT TIME ZONE.*\) AS TIMESTAMP\)/
    );
  });
});
