import { getEnv } from '@cubejs-backend/shared';
import { PostgresQuery } from '../../../src/adapter/PostgresQuery';
import { prepareJsCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

// Calculated measures (`type: number` over other measures of the same cube)
// combined with a dimension reached through a hasMany join. The fan-out forces
// every measure to pick a strategy: countDistinct survives row multiplication
// and is aggregated in place, sum has to go through the keys subquery. A
// calculated measure is neither - it is an expression over aggregates and can
// only be evaluated once its components have been re-aggregated.
describe('Calculated measure on the multi-fact path', () => {
  jest.setTimeout(200000);

  const { compiler, joinGraph, cubeEvaluator } = prepareJsCompiler(`
cube(\`Payments\`, {
  // id is TEXT on purpose: it is the operand that ends up in arithmetic when a
  // calculated measure loses the aggregation around its components.
  sql: \`
    SELECT 'p1' AS id, 'SUCCESS' AS status, 100 AS amount, 'EUR' AS currency UNION ALL
    SELECT 'p2' AS id, 'SUCCESS' AS status, 200 AS amount, 'EUR' AS currency UNION ALL
    SELECT 'p3' AS id, 'DECLINED' AS status, 300 AS amount, 'EUR' AS currency UNION ALL
    SELECT 'p4' AS id, 'SUCCESS' AS status, 400 AS amount, 'USD' AS currency
  \`,

  joins: {
    Meta: {
      relationship: \`hasMany\`,
      sql: \`\${CUBE}.id = \${Meta}.payment_id\`,
    },
    Rates: {
      relationship: \`belongsTo\`,
      sql: \`\${CUBE}.currency = \${Rates}.currency\`,
    },
  },

  measures: {
    count: {
      sql: \`id\`,
      type: \`countDistinct\`,
    },
    successCount: {
      sql: \`id\`,
      type: \`countDistinct\`,
      filters: [{ sql: \`\${CUBE}.status = 'SUCCESS'\` }],
    },
    totalAmount: {
      sql: \`amount\`,
      type: \`sum\`,
    },
    successAmount: {
      sql: \`amount\`,
      type: \`sum\`,
      filters: [{ sql: \`\${CUBE}.status = 'SUCCESS'\` }],
    },
    // Needs a join to Rates, and sum is not immune to the Meta fan-out, so it
    // takes the keys-subquery path.
    convertedValue: {
      sql: \`\${CUBE}.amount / nullif(\${Rates.fxRate}, 0)\`,
      type: \`sum\`,
    },
    // Calculated measures over components of the same cube. The components
    // differ in whether they survive row multiplication on their own:
    // countDistinct does, sum does not.
    successRate: {
      sql: \`100.0 * \${successCount} / nullif(\${count}, 0)\`,
      type: \`number\`,
    },
    successAmountRate: {
      sql: \`100.0 * \${successAmount} / nullif(\${totalAmount}, 0)\`,
      type: \`number\`,
    },
  },

  dimensions: {
    id: { sql: \`id\`, type: \`string\`, primaryKey: true },
    status: { sql: \`status\`, type: \`string\` },
  },
});

cube(\`Meta\`, {
  // p1 carries two meta rows so grouping by Meta.value multiplies it.
  sql: \`
    SELECT 'm1' AS id, 'p1' AS payment_id, 'A' AS value UNION ALL
    SELECT 'm1b' AS id, 'p1' AS payment_id, 'A' AS value UNION ALL
    SELECT 'm2' AS id, 'p2' AS payment_id, 'A' AS value UNION ALL
    SELECT 'm3' AS id, 'p3' AS payment_id, 'A' AS value UNION ALL
    SELECT 'm4' AS id, 'p4' AS payment_id, 'B' AS value
  \`,
  dimensions: {
    id: { sql: \`id\`, type: \`string\`, primaryKey: true },
    paymentId: { sql: \`payment_id\`, type: \`string\` },
    value: { sql: \`value\`, type: \`string\` },
  },
});

cube(\`Rates\`, {
  sql: \`
    SELECT 'EUR' AS currency, 1.0 AS fx_rate UNION ALL
    SELECT 'USD' AS currency, 2.0 AS fx_rate
  \`,
  dimensions: {
    currency: { sql: \`currency\`, type: \`string\`, primaryKey: true },
    fxRate: { sql: \`fx_rate\`, type: \`number\` },
  },
});
    `);

  async function runQuery(q) {
    await compiler.compile();
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, q);
    return dbRunner.testQuery(query.buildSqlAndParams());
  }

  async function expectQueryToFail(q) {
    await compiler.compile();
    try {
      const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, q);
      await dbRunner.testQuery(query.buildSqlAndParams());
    } catch (e: any) {
      return e.message as string;
    }
    throw new Error('Expected the query to fail, but it succeeded');
  }

  it('calculated measure alone, grouped by a fan-out dimension', async () => {
    expect(await runQuery({
      measures: ['Payments.successRate'],
      dimensions: ['Meta.value'],
      order: [{ id: 'Meta.value' }],
    })).toEqual([
      { meta__value: 'A', payments__success_rate: '66.6666666666666667' },
      { meta__value: 'B', payments__success_rate: '100.0000000000000000' },
    ]);
  });

  it('calculated measure components, grouped by a fan-out dimension', async () => {
    expect(await runQuery({
      measures: ['Payments.successCount', 'Payments.count'],
      dimensions: ['Meta.value'],
      order: [{ id: 'Meta.value' }],
    })).toEqual([
      { meta__value: 'A', payments__success_count: '2', payments__count: '3' },
      { meta__value: 'B', payments__success_count: '1', payments__count: '1' },
    ]);
  });

  it('joined measure next to a distinct count, grouped by a fan-out dimension', async () => {
    expect(await runQuery({
      measures: ['Payments.convertedValue', 'Payments.count'],
      dimensions: ['Meta.value'],
      order: [{ id: 'Meta.value' }],
    })).toEqual([
      { meta__value: 'A', payments__converted_value: '600.0000000000000000', payments__count: '3' },
      { meta__value: 'B', payments__converted_value: '200.0000000000000000', payments__count: '1' },
    ]);
  });

  it('calculated measure next to a joined measure, grouped by a fan-out dimension', async () => {
    const query = {
      measures: ['Payments.successRate', 'Payments.convertedValue'],
      dimensions: ['Meta.value'],
      order: [{ id: 'Meta.value' }],
    };

    if (!getEnv('nativeSqlPlanner')) {
      // The calculated measure is inlined into the ungrouped measure-join with
      // the aggregation around its components removed, leaving the TEXT id
      // column in arithmetic.
      expect(await expectQueryToFail(query)).toContain('operator does not exist: numeric * text');
      return;
    }

    expect(await runQuery(query)).toEqual([
      { meta__value: 'A', payments__success_rate: '66.6666666666666667', payments__converted_value: '600.0000000000000000' },
      { meta__value: 'B', payments__success_rate: '100.0000000000000000', payments__converted_value: '200.0000000000000000' },
    ]);
  });

  it('calculated measure over sums next to a joined measure, grouped by a fan-out dimension', async () => {
    const query = {
      measures: ['Payments.successAmountRate', 'Payments.convertedValue'],
      dimensions: ['Meta.value'],
      order: [{ id: 'Meta.value' }],
    };

    if (!getEnv('nativeSqlPlanner')) {
      // Types line up here, so the failure surfaces one step later: the
      // calculated measure is projected without an aggregate and without being
      // grouped.
      expect(await expectQueryToFail(query)).toContain('must appear in the GROUP BY clause');
      return;
    }

    expect(await runQuery(query)).toEqual([
      { meta__value: 'A', payments__success_amount_rate: '50.0000000000000000', payments__converted_value: '600.0000000000000000' },
      { meta__value: 'B', payments__success_amount_rate: '100.0000000000000000', payments__converted_value: '200.0000000000000000' },
    ]);
  });

  it('calculated measure next to a joined measure, without a fan-out dimension', async () => {
    expect(await runQuery({
      measures: ['Payments.successRate', 'Payments.convertedValue'],
      dimensions: ['Payments.status'],
      order: [{ id: 'Payments.status' }],
    })).toEqual([
      { payments__status: 'DECLINED', payments__success_rate: '0.00000000000000000000', payments__converted_value: '300.0000000000000000' },
      { payments__status: 'SUCCESS', payments__success_rate: '100.0000000000000000', payments__converted_value: '500.0000000000000000' },
    ]);
  });
});
