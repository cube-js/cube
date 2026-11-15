import { BigqueryQuery } from '../../src/adapter/BigqueryQuery';
import { prepareJsCompiler } from './PrepareCompiler';

describe('BigqueryQuery', () => {
  const compilers = prepareJsCompiler(`
    cube('fact', {
      sql: \`SELECT * FROM fact\`,
      measures: {
        count: { type: 'count' }
      },
      dimensions: {
        id: { sql: 'id', type: 'number', primaryKey: true },
        orgId: { sql: 'org_id', type: 'number' }
      }
    });
  `);

  // https://github.com/cube-js/cube/issues/10083
  it.each([
    ['legacy planner', false],
    ['Tesseract', true],
  ])('does not cast an equals filter value on a number dimension to FLOAT64 (%s)', async (_planner, useNativeSqlPlanner) => {
    await compilers.compile();

    const query = compilers.createQuery(BigqueryQuery, {
      measures: ['fact.count'],
      filters: [{ member: 'fact.orgId', operator: 'equals', values: ['4'] }],
      useNativeSqlPlanner,
    });
    const [sql] = query.buildSqlAndParams();

    // Comparing an INT64 clustering column with a FLOAT64 value disables cluster pruning
    expect(sql).toMatch(/`fact`\.org_id = /);
    expect(sql).not.toContain('AS FLOAT64');
  });
});
