import { PostgresQuery } from '../../../src/adapter';
import { prepareYamlCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

// Built from a cube name longer than 63 chars, two subquery aliases would
// truncate to the same Postgres identifier; `sql_alias` keeps them distinct.
describe('Subquery dimensions of a cube with sql_alias', () => {
  jest.setTimeout(200000);

  const compilers = prepareYamlCompiler(`
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
    await compilers.compile();
    const query = compilers.createQuery(PostgresQuery, q);
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
