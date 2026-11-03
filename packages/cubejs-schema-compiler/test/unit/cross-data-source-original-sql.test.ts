import { prepareJsCompiler } from './PrepareCompiler';
import { PostgresQuery } from '../../src';

// https://github.com/cube-js/cube/issues/12571
describe('cross-data-source original_sql pre-aggregations', () => {
  const originalPlannerEnv = process.env.CUBEJS_TESSERACT_SQL_PLANNER;

  afterAll(() => {
    if (originalPlannerEnv === undefined) {
      delete process.env.CUBEJS_TESSERACT_SQL_PLANNER;
    } else {
      process.env.CUBEJS_TESSERACT_SQL_PLANNER = originalPlannerEnv;
    }
  });

  for (const tesseract of [false, true]) {
    it(`does not recurse in detectDataSource (tesseract=${tesseract})`, async () => {
      process.env.CUBEJS_TESSERACT_SQL_PLANNER = String(tesseract);
      const compilers = prepareJsCompiler(`
cube(\`employees\`, {
  data_source: \`snowflake\`,
  sql: \`SELECT * FROM employees\`,
  pre_aggregations: { main: { type: \`original_sql\`, external: true, indexes: { by_department: { columns: [\`department\`] } } } },
  joins: {
    department_map: { sql: \`\${CUBE}.department = \${department_map.source_value}\`, relationship: \`many_to_one\` },
  },
  measures: { count: { type: \`count\` } },
  dimensions: {
    id: { sql: \`id\`, type: \`string\`, primary_key: true },
    department: { sql: \`COALESCE(\${department_map.value}, \${CUBE}.department)\`, type: \`string\` },
  },
});
cube(\`department_map\`, {
  data_source: \`default\`,
  sql: \`SELECT existing_value AS source_value, new_value AS value FROM mapping\`,
  pre_aggregations: { main: { type: \`original_sql\`, external: true, indexes: { by_source: { columns: [\`source_value\`] } } } },
  dimensions: {
    source_value: { sql: \`source_value\`, type: \`string\`, primary_key: true },
    value: { sql: \`value\`, type: \`string\` },
  },
});
`);
      await compilers.compiler.compile();
      const query = compilers.createQuery(PostgresQuery, {
        measures: ['employees.count'],
        dimensions: ['employees.department'],
        externalQueryClass: PostgresQuery,
      });
      expect(query.dataSource).toEqual('snowflake');
      const descriptions = query.preAggregations.preAggregationsDescription();
      expect(descriptions.map(p => [p.preAggregationId, p.dataSource])).toEqual([
        ['department_map.main', 'default'],
        ['employees.main', 'snowflake'],
      ]);
      const [sql] = query.buildSqlAndParams();
      expect(sql).toContain('employees_main');
      expect(sql).toContain('department_map_main');
    });
  }
});
