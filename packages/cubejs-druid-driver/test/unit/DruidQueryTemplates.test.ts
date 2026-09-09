import { prepareCompiler as originalPrepareCompiler } from '@cubejs-backend/schema-compiler';
import { DruidQuery } from '../../src/DruidQuery';

const prepareCompiler = (content: string) => originalPrepareCompiler({
  localPath: () => __dirname,
  dataSchemaFiles: () => Promise.resolve([{ fileName: 'main.js', content }]),
}, { adapter: 'postgres' });

const MODEL = `
  cube('orders', {
    sql_table: 'orders',

    measures: {
      count: {
        type: 'count',
      },
    },

    dimensions: {
      id: {
        sql: 'id',
        type: 'number',
        primary_key: true,
      },
      status: {
        sql: 'status',
        type: 'string',
      },
    },
  });
`;

describe('DruidQuery SQL templates', () => {
  // Druid's LIKE has no default escape character, so the value escaping both
  // planners apply (BaseQuery's `like_escape_char`, mirroring
  // `BaseFilter.escapeWildcardChars`) only means anything if the statement
  // carries the clause that interprets it. Without one a user searching for a
  // literal `%` matches nothing instead of the rows containing a percent sign.
  it.each([['legacy', false], ['tesseract', true]])(
    'escapes LIKE wildcards in filter values and interprets them on the %s planner',
    async (_name, useNativeSqlPlanner) => {
      const { compiler, joinGraph, cubeEvaluator } = prepareCompiler(MODEL);

      await compiler.compile();

      const query = new DruidQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: ['orders.count'],
        filters: [{ member: 'orders.status', operator: 'contains', values: ['%'] }],
        useNativeSqlPlanner,
      });

      const [sql, params] = query.buildSqlAndParams();

      expect(params).toEqual(['\\%']);
      // eslint-disable-next-line quotes -- double quotes keep the SQL readable
      expect(sql).toContain("ESCAPE '\\'");
    }
  );
});
