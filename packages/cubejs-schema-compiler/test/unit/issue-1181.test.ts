import { prepareJsCompiler } from './PrepareCompiler';
import { PostgresQuery } from '../../src/adapter/PostgresQuery';

// https://github.com/cube-js/cube/issues/1181
// An external (Cube Store) `originalSql` pre-aggregation gets joined to raw data of
// another cube in a query that runs against the source database. The generated SQL
// joins `analytics_db.t1` with the Cube Store table `stb_pre_aggregations.companies_main`,
// which fails at runtime with an obscure "relation does not exist" error.
// Cube should either reject such a query with a descriptive error or not use the
// external pre-aggregation in a source-database query.
describe('Issue #1181: external originalSql pre-aggregation joined to raw data', () => {
  const { compiler, joinGraph, cubeEvaluator } = prepareJsCompiler(`
    cube('t1', {
      sql: 'SELECT * FROM analytics_db.t1',
      joins: {
        companies: {
          relationship: 'many_to_one',
          sql: \`\${CUBE}.company_id = \${companies}.id\`,
        },
      },
      measures: {
        sales: { sql: 'sales', type: 'sum' },
      },
      dimensions: {
        id: { sql: 'id', type: 'number', primaryKey: true },
        report_date: { sql: 'report_date', type: 'time' },
      },
    });

    cube('companies', {
      sql: 'SELECT * FROM companies',
      measures: {
        count: { type: 'count' },
      },
      dimensions: {
        id: { sql: 'id', type: 'number', primaryKey: true },
        company_id: { sql: 'company_id', type: 'string' },
      },
      preAggregations: {
        main: {
          type: 'originalSql',
          external: true,
        },
      },
    });
  `);

  it('does not join an external originalSql pre-aggregation into a source-database query', async () => {
    await compiler.compile();
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: ['t1.sales'],
      timeDimensions: [{ dimension: 't1.report_date', granularity: 'day', dateRange: ['2020-01-01', '2020-01-31'] }],
      filters: [{ member: 'companies.company_id', operator: 'equals', values: ['1'] }],
      timezone: 'UTC',
    });

    let sql: string;

    try {
      [sql] = query.buildSqlAndParams();
    } catch (e: any) {
      // A descriptive error is an acceptable fix
      expect(e.message).toMatch(/original ?sql/i);
      return;
    }

    expect(query.externalPreAggregationQuery()).toBe(false);
    expect(sql).not.toContain('stb_pre_aggregations.companies_main');
  });
});
