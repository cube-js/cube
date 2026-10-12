import { CubeStoreQuery, prepareCompiler as originalPrepareCompiler } from '@cubejs-backend/schema-compiler';
import { DatabricksQuery } from '../src/DatabricksQuery';

const prepareCompiler = (content: string) => originalPrepareCompiler({
  localPath: () => __dirname,
  dataSchemaFiles: () => Promise.resolve([{ fileName: 'main.js', content }]),
});

const ISO_VALUES = ['2026-06-24T00:00:00.000Z', '2026-06-25T00:00:00.000Z'];
const CAST_PARAM = 'from_utc_timestamp(replace(replace(?, \'T\', \' \'), \'Z\', \'\'), \'UTC\')';
const CAST_PARAMS = `(${CAST_PARAM}, ${CAST_PARAM})`;

describe('DatabricksQuery', () => {
  const { compiler, joinGraph, cubeEvaluator } = prepareCompiler(
    `
cube(\`sales\`, {
  sql: \` select * from public.sales \`,

  measures: {
    count: {
      type: 'count'
    },
    // Only the pre-aggregation test queries it, so no other query matches the rollup
    rollupCount: {
      type: 'count'
    },
    lastSale: {
      type: 'time',
      sql: 'max(sold_at)'
    }
  },
  dimensions: {
    category: {
      type: 'string',
      sql: 'category'
    },
    soldAt: {
      type: 'time',
      sql: 'sold_at'
    },
    // A STRING column holding 'yyyy-MM-dd' values
    soldDay: {
      type: 'time',
      sql: 'sold_day'
    },
  },
  preAggregations: {
    byDay: {
      measures: [CUBE.rollupCount],
      dimensions: [CUBE.soldAt],
    }
  }
});
`,
  );

  const buildSqlAndParams = (filters: any[], useNativeSqlPlanner: boolean, options: any = {}) => {
    const query = new DatabricksQuery(
      { joinGraph, cubeEvaluator, compiler },
      {
        measures: ['sales.count'],
        filters,
        timezone: 'UTC',
        useNativeSqlPlanner,
        ...options,
      }
    );
    return query.buildSqlAndParams();
  };

  describe.each([
    ['tesseract', true],
    ['legacy', false],
  ])('%s planner', (_name, useNativeSqlPlanner) => {
    beforeAll(() => compiler.compile());

    it.each([
      ['equals', 'IN'],
      ['in', 'IN'],
      ['notEquals', 'NOT IN'],
      ['notIn', 'NOT IN'],
    ])('casts both sides of a time dimension %s list', (operator, keyword) => {
      const [sql, params] = buildSqlAndParams(
        [{ member: 'sales.soldAt', operator, values: ISO_VALUES }],
        useNativeSqlPlanner,
      );

      expect(sql).toContain(`try_cast(\`sales\`.sold_at AS TIMESTAMP) ${keyword} ${CAST_PARAMS}`);
      expect(params).toEqual(ISO_VALUES);
    });

    it('casts the column of a string-backed time dimension too', () => {
      const [sql] = buildSqlAndParams(
        [{ member: 'sales.soldDay', operator: 'in', values: ['2026-06-24', '2026-06-25'] }],
        useNativeSqlPlanner,
      );

      expect(sql).toContain(`try_cast(\`sales\`.sold_day AS TIMESTAMP) IN ${CAST_PARAMS}`);
    });

    it('checks the uncast column for null', () => {
      const [sql] = buildSqlAndParams(
        [{ member: 'sales.soldAt', operator: 'in', values: [...ISO_VALUES, null] }],
        useNativeSqlPlanner,
      );

      expect(sql).toContain(`IN ${CAST_PARAMS} OR \`sales\`.sold_at IS NULL`);
    });

    it('leaves a single-value time dimension equals as is', () => {
      const [sql] = buildSqlAndParams(
        [{ member: 'sales.soldAt', operator: 'equals', values: [ISO_VALUES[0]] }],
        useNativeSqlPlanner,
      );

      expect(sql).toContain('`sales`.sold_at = ?');
    });

    it('leaves string dimension and time measure lists as is', () => {
      const [sql] = buildSqlAndParams(
        [
          { member: 'sales.category', operator: 'in', values: ['a', 'b'] },
          { member: 'sales.lastSale', operator: 'in', values: ISO_VALUES },
        ],
        useNativeSqlPlanner,
      );

      expect(sql).toContain('`sales`.category IN (?, ?)');
      expect(sql).toMatch(/max\((`sales`\.)?sold_at\)\)? IN \(\?, \?\)/);
      expect(sql).not.toMatch(/cast\(/i);
    });

    it('leaves a time dimension list as is in Cube Store pre-aggregation SQL', () => {
      const [sql] = buildSqlAndParams(
        [{ member: 'sales.soldAt', operator: 'in', values: ISO_VALUES }],
        useNativeSqlPlanner,
        { measures: ['sales.rollupCount'], dimensions: ['sales.soldAt'], externalQueryClass: CubeStoreQuery },
      );

      expect(sql).toContain('sales_by_day');
      expect(sql).toMatch(/sales__sold_at[`"] IN \(\?, \?\)/);
      expect(sql).not.toMatch(/cast\(/i);
    });
  });
});
