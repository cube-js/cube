import { getEnv } from '@cubejs-backend/shared';
import { prepareJsCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

// https://github.com/cube-js/cube/issues/10864
// Tesseract: outer CTE of a multi_stage `type: number` measure with inline
// aggregate SQL (STDDEV({x}) / NULLIF(AVG({x}), 0)) + add_group_by is emitted
// without GROUP BY when the query has a dimension.
describe('Issue #10864: multi_stage type:number inline aggregate + add_group_by', () => {
  jest.setTimeout(200000);

  const { compiler, joinGraph, cubeEvaluator } = prepareJsCompiler(`
cube(\`Orders\`, {
  sql: \`
    SELECT 1 AS id, 100 AS amount, 'new' AS status UNION ALL
    SELECT 2 AS id, 200 AS amount, 'new' AS status UNION ALL
    SELECT 3 AS id, 300 AS amount, 'processed' AS status UNION ALL
    SELECT 4 AS id, 500 AS amount, 'processed' AS status UNION ALL
    SELECT 5 AS id, 600 AS amount, 'shipped' AS status
  \`,
  measures: {
    amount_count: {
      sql: \`amount\`,
      type: \`count\`,
    },
    amount_sum: {
      sql: \`amount\`,
      type: \`sum\`,
    },
    cv_amount: {
      sql: \`STDDEV(\${amount_count}) / NULLIF(AVG(\${amount_count}), 0)\`,
      type: \`number\`,
      multi_stage: true,
      add_group_by: [CUBE.id],
    },
    cv_amount_sum: {
      sql: \`ROUND((STDDEV(\${amount_sum}) / NULLIF(AVG(\${amount_sum}), 0))::numeric, 4)\`,
      type: \`number\`,
      multi_stage: true,
      add_group_by: [CUBE.id],
    },
    max_amount: {
      sql: \`\${amount_count}\`,
      type: \`max\`,
      multi_stage: true,
      add_group_by: [CUBE.id],
    },
  },
  dimensions: {
    id: {
      sql: \`id\`,
      type: \`number\`,
      primary_key: true,
      public: true,
    },
    status: {
      sql: \`status\`,
      type: \`string\`,
    },
  },
});
  `);

  if (getEnv('nativeSqlPlanner')) {
    it('control: dedicated aggregate type (max) works', async () => dbRunner.runQueryTest({
      measures: ['Orders.max_amount'],
      dimensions: ['Orders.status'],
      order: [{ id: 'Orders.status' }],
      timezone: 'UTC',
    }, [
      { orders__status: 'new', orders__max_amount: '1' },
      { orders__status: 'processed', orders__max_amount: '1' },
      { orders__status: 'shipped', orders__max_amount: '1' },
    ], { joinGraph, cubeEvaluator, compiler }));

    it('type:number STDDEV/AVG over count measure (exact issue schema)', async () => dbRunner.runQueryTest({
      measures: ['Orders.cv_amount'],
      dimensions: ['Orders.status'],
      order: [{ id: 'Orders.status' }],
      timezone: 'UTC',
    }, [
      { orders__status: 'new', orders__cv_amount: '0' },
      { orders__status: 'processed', orders__cv_amount: '0' },
      { orders__status: 'shipped', orders__cv_amount: null },
    ], { joinGraph, cubeEvaluator, compiler }));

    it('type:number STDDEV/AVG over sum measure', async () => dbRunner.runQueryTest({
      measures: ['Orders.cv_amount_sum'],
      dimensions: ['Orders.status'],
      order: [{ id: 'Orders.status' }],
      timezone: 'UTC',
    }, [
      { orders__status: 'new', orders__cv_amount_sum: '0.4714' },
      { orders__status: 'processed', orders__cv_amount_sum: '0.3536' },
      { orders__status: 'shipped', orders__cv_amount_sum: null },
    ], { joinGraph, cubeEvaluator, compiler }));

    it('type:number STDDEV/AVG without dimensions (collapsed)', async () => dbRunner.runQueryTest({
      measures: ['Orders.cv_amount_sum'],
      timezone: 'UTC',
    }, [
      { orders__cv_amount_sum: '0.6099' },
    ], { joinGraph, cubeEvaluator, compiler }));
  } else {
    test.skip('Tesseract only', () => { expect(1).toBe(1); });
  }
});
