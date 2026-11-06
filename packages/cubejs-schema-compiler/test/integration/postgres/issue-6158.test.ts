// Reproduces https://github.com/cube-js/cube/issues/6158: a `case` dimension with an
// empty `when` list compiles, but renders `CASE ELSE 'Unknown' END`, which is not
// valid SQL. It should either be rejected at compile time or render valid SQL.
import { PostgresQuery } from '../../../src/adapter/PostgresQuery';
import { prepareYamlCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

describe('Case dimension without when branches', () => {
  jest.setTimeout(200000);

  const compilers = prepareYamlCompiler(`
cubes:
  - name: sessions
    sql: SELECT 1 AS id
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: area
        type: string
        case:
          when: []
          else:
            label: Unknown
`);

  it('renders valid SQL', async () => {
    await compilers.compiler.compile();
    const query = new PostgresQuery(compilers, {
      dimensions: ['sessions.area'],
      timezone: 'UTC',
    });

    const res = await dbRunner.testQuery(query.buildSqlAndParams());
    expect(res).toEqual([{ sessions__area: 'Unknown' }]);
  });
});
