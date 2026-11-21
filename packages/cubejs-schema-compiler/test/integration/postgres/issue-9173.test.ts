// Reproduces https://github.com/cube-js/cube/issues/9173: FILTER_GROUP inside a
// measure filter recurses until "RangeError: Maximum call stack size exceeded".
//
// Each test builds its own compiler on purpose: `collectMemberNamesFor` results are
// kept in the compiler-wide cache, so a previous query without filters would
// populate it and hide the recursion for later queries on the same compiler.
import { PostgresQuery } from '../../../src/adapter/PostgresQuery';
import { prepareJsCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

describe('FILTER_GROUP in member SQL', () => {
  jest.setTimeout(200000);

  const model = (filterSql: string) => `
    cube('test', {
      sql: \`select 1 as value, '1' as name union all select 2 as value, '2' as name\`,
      measures: {
        metric: {
          sql: 'value',
          type: 'avg',
          filters: [{ sql: \`${filterSql}\` }],
        },
      },
      dimensions: {
        name: { sql: 'name', type: 'string' },
        value: { sql: 'value', type: 'number' },
      },
    });
  `;

  const query = {
    measures: ['test.metric'],
    dimensions: ['test.name'],
    filters: [{ member: 'test.name', operator: 'equals', values: ['1'] }],
    timezone: 'UTC',
  };

  // Mirrors CompilerApi.getSqlGenerator(): the server resolves `dataSource`
  // (-> allCubeNames -> collectCubeNames) inside `compiler.withQuery()` before
  // building SQL. That JS traversal runs for both planners, so with Tesseract
  // the overflow only shows up through it, not through buildSqlAndParams().
  const run = async (filterSql: string) => {
    const compilers = prepareJsCompiler(model(filterSql));
    await compilers.compiler.compile();
    const pgQuery = new PostgresQuery(compilers, query);
    const dataSource = compilers.compiler.withQuery(pgQuery, () => pgQuery.dataSource);
    expect(dataSource).toBe('default');
    const res = await dbRunner.testQuery(pgQuery.buildSqlAndParams());
    expect(res).toEqual([{ test__name: '1', test__metric: '1.00000000000000000000' }]);
  };

  it('renders a top-level FILTER_GROUP in a measure filter', async () => run(
    // eslint-disable-next-line no-template-curly-in-string
    '${FILTER_GROUP(FILTER_PARAMS.test.name.filter((v) => `${CUBE}.name = ${v}`))}'
  ));

  // The exact shape from the issue report.
  it('renders FILTER_GROUP as a value in a measure filter (issue #9173)', async () => run(
    // eslint-disable-next-line no-template-curly-in-string
    '${CUBE}.name = ${FILTER_GROUP(FILTER_PARAMS.test.name.filter((kpi) => kpi))}'
  ));
});
