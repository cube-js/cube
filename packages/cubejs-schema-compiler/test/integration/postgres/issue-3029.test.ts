import { PostgresQuery } from '../../../src/adapter/PostgresQuery';
import { prepareJsCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

// https://github.com/cube-js/cube/issues/3029
// A calculated `number` measure over an additive one (`billmin = billsec / 60`)
// is served by a plain rollup that only stores `billsec`. The same measure
// listed in a `rollupJoin` is matched too, but the rendered SQL selects a
// `calls__billmin` column that the underlying `calls_rollup` never stored. On a
// running Cube + Cube Store v1.7.47 this fails with
// `Schema error: No field named calls__billmin`.
describe('Issue #3029: rollupJoin with a calculated measure', () => {
  jest.setTimeout(200000);

  const compilers = prepareJsCompiler(`
    cube('calls', {
      sql: \`
        SELECT 1 AS id, 0 AS customer_id, 60 AS billsec UNION ALL
        SELECT 2 AS id, 0 AS customer_id, 120 AS billsec UNION ALL
        SELECT 3 AS id, 1 AS customer_id, 300 AS billsec
      \`,
      joins: {
        customers: {
          relationship: 'many_to_one',
          sql: \`\${CUBE.customer_id} = \${customers.id}\`,
        },
      },
      measures: {
        billsec: { sql: 'billsec', type: 'sum' },
        billmin: { sql: \`\${CUBE.billsec} / 60\`, type: 'number' },
      },
      dimensions: {
        id: { sql: 'id', type: 'number', primaryKey: true },
        customer_id: { sql: 'customer_id', type: 'number' },
      },
      preAggregations: {
        calls_rollup: {
          type: 'rollup',
          measures: [CUBE.billsec],
          dimensions: [CUBE.customer_id],
        },
        calls_customers: {
          type: 'rollupJoin',
          measures: [CUBE.billsec, CUBE.billmin],
          dimensions: [customers.name],
          rollups: [customers.customers_rollup, CUBE.calls_rollup],
        },
      },
    });

    cube('customers', {
      sql: \`SELECT 0 AS id, 'a' AS name UNION ALL SELECT 1 AS id, 'b' AS name\`,
      dimensions: {
        id: { sql: 'id', type: 'number', primaryKey: true },
        name: { sql: 'name', type: 'string' },
      },
      preAggregations: {
        customers_rollup: {
          type: 'rollup',
          dimensions: [CUBE.id, CUBE.name],
        },
      },
    });
  `);

  // Postgres returns `numeric` as a string with a driver-specific scale.
  const billmin = (rows: any[]) => rows.map(r => ({ ...r, calls__billmin: Number(r.calls__billmin) }));

  it('a plain rollup serves the calculated measure from its components', async () => {
    await compilers.compiler.compile();

    const query = new PostgresQuery(compilers, {
      measures: ['calls.billmin'],
      dimensions: ['calls.customer_id'],
      order: [{ id: 'calls.customer_id' }],
      timezone: 'UTC',
      preAggregationsSchema: '',
    });

    expect(query.preAggregations?.findPreAggregationForQuery()?.preAggregationName).toEqual('calls_rollup');

    const res = await dbRunner.evaluateQueryWithPreAggregations(query);
    expect(billmin(res)).toEqual([
      { calls__customer_id: 0, calls__billmin: 3 },
      { calls__customer_id: 1, calls__billmin: 5 },
    ]);
  });

  it('a rollupJoin serves the calculated measure from its components', async () => {
    await compilers.compiler.compile();

    const query = new PostgresQuery(compilers, {
      measures: ['calls.billmin'],
      dimensions: ['customers.name'],
      order: [{ id: 'customers.name' }],
      timezone: 'UTC',
      preAggregationsSchema: '',
    });

    expect(query.preAggregations?.findPreAggregationForQuery()?.preAggregationName).toEqual('calls_customers');

    const res = await dbRunner.evaluateQueryWithPreAggregations(query);
    expect(billmin(res)).toEqual([
      { customers__name: 'a', calls__billmin: 3 },
      { customers__name: 'b', calls__billmin: 5 },
    ]);
  });
});
