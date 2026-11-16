// Reproduces https://github.com/cube-js/cube/issues/8561: the join alias of a
// subquery dimension is built from the cube name and ignores `sql_alias`. With a
// long cube name, Postgres truncates every such alias to 63 characters, so two
// subquery dimensions of the same cube end up with the same alias and the query
// fails with "table name ... specified more than once".
import { PostgresQuery } from '../../../src/adapter/PostgresQuery';
import { prepareYamlCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

const LONG_NAME = 'my_cube_with_a_very_long_name_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx';

describe('Subquery dimensions on a cube with sql_alias', () => {
  jest.setTimeout(200000);

  const compilers = prepareYamlCompiler(`
cubes:
  - name: aux
    sql: SELECT 1 AS id, 123 AS value
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
    measures:
      - name: count
        type: count

  - name: ${LONG_NAME}
    sql: SELECT 1 AS id, 123 AS value
    sql_alias: x
    joins:
      - name: aux
        relationship: one_to_one
        sql: "{CUBE.id} = {aux.id}"
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: aux_count
        sql: "{aux.count}"
        type: number
        sub_query: true
      - name: aux_count_two
        sql: "{aux.count}"
        type: number
        sub_query: true
`);

  it('uses sql_alias for subquery join aliases', async () => {
    await compilers.compiler.compile();
    const query = new PostgresQuery(compilers, {
      dimensions: [`${LONG_NAME}.aux_count`, `${LONG_NAME}.aux_count_two`],
      timezone: 'UTC',
    });
    const res = await dbRunner.testQuery(query.buildSqlAndParams());
    expect(res).toEqual([{ x__aux_count: '1', x__aux_count_two: '1' }]);

    const [sql] = query.buildSqlAndParams();
    expect(sql).not.toContain(LONG_NAME);
  });
});
