import { prepareJsCompiler } from './PrepareCompiler';
import { PostgresQuery } from '../../src/adapter/PostgresQuery';

/**
 * Pre-aggregation references built by interpolating the cube itself
 * (`` (CUBE) => `${CUBE}.issued_date` ``) instead of a member. The cube
 * stringifies to its name, so such a reference names the member as text; both
 * planners have to resolve it to the same member.
 */
describe('pre-aggregation references interpolating the cube', () => {
  const model = (preAggregations: string) => `
    const getCubeFields = (cube, names) => names.map((name) => cube[name]);

    cube('invoices', {
      sql: 'SELECT * FROM invoices',

      measures: {
        count: { type: 'count' },
        total: { sql: 'amount', type: 'sum' },
      },

      dimensions: {
        id: { sql: 'id', type: 'number', primaryKey: true },
        org_id: { sql: 'org_id', type: 'string' },
        issued_date: { sql: 'issued_date', type: 'time' },
        payment_received_date: { sql: 'payment_received_date', type: 'time' },
      },

      preAggregations: ${preAggregations},
    });
  `;

  const preAggregationsWithInterpolatedTimeDimension = `{
    by_org_and_issued_date: {
      type: 'rollup',
      measures: (CUBE) => getCubeFields(CUBE, ['count', 'total']),
      dimensions: (CUBE) => getCubeFields(CUBE, ['org_id']),
      timeDimension: (CUBE) => \`\${CUBE}.issued_date\`,
      granularity: 'day',
      partitionGranularity: 'month',
    },
    by_org_and_payment_received_date: {
      type: 'rollup',
      measures: (CUBE) => getCubeFields(CUBE, ['count', 'total']),
      dimensions: (CUBE) => getCubeFields(CUBE, ['org_id']),
      timeDimension: (CUBE) => \`\${CUBE}.payment_received_date\`,
      granularity: 'day',
      partitionGranularity: 'month',
    },
    by_org_all_time: {
      type: 'rollup',
      measures: (CUBE) => getCubeFields(CUBE, ['count', 'total']),
      dimensions: (CUBE) => getCubeFields(CUBE, ['org_id']),
    },
  }`;

  const preAggregationsWithInterpolatedMembers = `{
    by_org_and_issued_date: {
      type: 'rollup',
      measures: (CUBE) => [\`\${CUBE}.count\`, \`\${CUBE}.total\`],
      dimensions: (CUBE) => [\`\${CUBE}.org_id\`],
      timeDimension: (CUBE) => \`\${CUBE}.issued_date\`,
      granularity: 'day',
      partitionGranularity: 'month',
    },
  }`;

  const preAggregationWithGranularitySuffix = `{
    by_org_and_issued_date: {
      type: 'rollup',
      measures: (CUBE) => getCubeFields(CUBE, ['count']),
      timeDimension: (CUBE) => \`\${CUBE}.issued_date_day\`,
      granularity: 'day',
      partitionGranularity: 'month',
    },
  }`;

  async function buildQuery(preAggregations: string, useNativeSqlPlanner: boolean) {
    const { compiler, joinGraph, cubeEvaluator } = prepareJsCompiler(model(preAggregations));
    await compiler.compile();

    return new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: ['invoices.count'],
      dimensions: ['invoices.org_id'],
      timeDimensions: [{
        dimension: 'invoices.issued_date',
        granularity: 'day',
        dateRange: ['2020-01-01', '2020-03-31'],
      }],
      timezone: 'UTC',
      useNativeSqlPlanner,
    });
  }

  for (const useNativeSqlPlanner of [false, true]) {
    const planner = useNativeSqlPlanner ? 'tesseract' : 'legacy';

    it(`resolves an interpolated time dimension (${planner})`, async () => {
      const query = await buildQuery(preAggregationsWithInterpolatedTimeDimension, useNativeSqlPlanner);
      query.buildSqlAndParams();

      const descriptions: any = query.preAggregations?.preAggregationsDescription();
      expect(descriptions.map(d => d.preAggregationId)).toEqual(['invoices.by_org_and_issued_date']);
    });

    it(`resolves interpolated measure and dimension references (${planner})`, async () => {
      const query = await buildQuery(preAggregationsWithInterpolatedMembers, useNativeSqlPlanner);
      query.buildSqlAndParams();

      const descriptions: any = query.preAggregations?.preAggregationsDescription();
      expect(descriptions.map(d => d.preAggregationId)).toEqual(['invoices.by_org_and_issued_date']);
    });

    it(`reports the member an interpolated reference names when it does not exist (${planner})`, async () => {
      const query = await buildQuery(preAggregationWithGranularitySuffix, useNativeSqlPlanner);

      expect(() => query.buildSqlAndParams()).toThrow(
        /'issued_date_day' not found for path 'invoices.issued_date_day'/
      );
    });
  }
});
