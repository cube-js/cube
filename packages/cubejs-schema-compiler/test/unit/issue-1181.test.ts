import { PostgresQuery } from '../../src';
import { prepareJsCompiler } from './PrepareCompiler';

// https://github.com/cube-js/cube/issues/1181
// An external originalSql pre-aggregation joined with a cube that has no
// pre-aggregation can't be served by Cube Store: the raw table of the other cube
// doesn't exist there. Cube should fail with a descriptive error instead of
// routing the query to Cube Store, which fails with `Table ... was not found`.
describe('external originalSql pre-aggregation joined with raw data (issue #1181)', () => {
  const { compiler, joinGraph, cubeEvaluator } = prepareJsCompiler(`
    cube(\`t1\`, {
      sqlTable: \`analytics.t1\`,
      joins: {
        companies: {
          sql: \`\${CUBE}.company_id = \${companies}.id\`,
          relationship: \`many_to_one\`,
        },
      },
      measures: {
        sales: { sql: \`sales\`, type: \`sum\` },
      },
      dimensions: {
        id: { sql: \`id\`, type: \`number\`, primaryKey: true },
        reportDate: { sql: \`report_date\`, type: \`time\` },
      },
    });

    cube(\`companies\`, {
      sqlTable: \`analytics.companies\`,
      dimensions: {
        id: { sql: \`id\`, type: \`number\`, primaryKey: true },
        companyId: { sql: \`company_id\`, type: \`string\` },
      },
      preAggregations: {
        main: { type: \`originalSql\`, external: true },
      },
    });
  `);

  beforeAll(async () => {
    await compiler.compile();
  });

  it('does not send raw tables to the external database', () => {
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: ['t1.sales'],
      timeDimensions: [{ dimension: 't1.reportDate', granularity: 'day' }],
      filters: [{ member: 'companies.companyId', operator: 'equals', values: ['a'] }],
      externalQueryClass: PostgresQuery,
      preAggregationsSchema: '',
    });

    let sql: string | undefined;
    let error: Error | undefined;

    try {
      [sql] = query.buildSqlAndParams();
    } catch (e: any) {
      error = e;
    }

    if (error) {
      expect(error.message).toMatch(/companies\.main/);
    } else {
      // Without an error the query must stay on the source database.
      expect(query.externalPreAggregationQuery()).toBe(false);
      expect(sql).toMatch(/analytics\.t1/);
    }
  });
});
