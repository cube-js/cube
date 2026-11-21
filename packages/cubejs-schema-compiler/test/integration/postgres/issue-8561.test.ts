import { PostgresQuery } from '../../../src/adapter/PostgresQuery';
import { prepareYamlCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

// https://github.com/cube-js/cube/issues/8561
// Subquery dimension aliases are built from the cube name instead of `sql_alias`.
// With a long cube name, Postgres truncates identifiers to 63 chars and the
// truncated aliases collide: `table name "..." specified more than once`.
describe('Issue 8561: subquery dimensions respect sql_alias', () => {
  jest.setTimeout(200000);

  const { compiler, joinGraph, cubeEvaluator } = prepareYamlCompiler(`
cubes:
  - name: my_auxiliary_cube
    sql: SELECT 1 AS id, 123 AS value
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
    measures:
      - name: count
        type: count
      - name: total
        sql: value
        type: sum

  - name: my_cube_with_a_very_long_name_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
    sql: SELECT 1 AS id, 123 AS value
    sql_alias: x
    joins:
      - name: my_auxiliary_cube
        relationship: one_to_one
        sql: "{CUBE.id} = {my_auxiliary_cube.id}"
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: aux_count
        sql: "{my_auxiliary_cube.count}"
        type: number
        sub_query: true
      - name: aux_total
        sql: "{my_auxiliary_cube.total}"
        type: number
        sub_query: true
`);

  async function runQueryTest(q: any, expectedResult: any) {
    await compiler.compile();
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, q);
    const res = await dbRunner.testQuery(query.buildSqlAndParams());
    expect(res).toEqual(expectedResult);
  }

  it('single subquery dimension', async () => runQueryTest({
    dimensions: ['my_cube_with_a_very_long_name_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx.aux_count'],
  }, [{ x__aux_count: '1' }]));

  it('two subquery dimensions', async () => runQueryTest({
    dimensions: [
      'my_cube_with_a_very_long_name_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx.aux_count',
      'my_cube_with_a_very_long_name_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx.aux_total',
    ],
  }, [{ x__aux_count: '1', x__aux_total: '123' }]));
});
