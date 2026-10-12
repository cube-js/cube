import { PostgresQuery } from '../../src/adapter/PostgresQuery';
import { prepareYamlCompiler } from './PrepareCompiler';

// #10371: a view join_path segment that names a joined cube which has the same
// name as a dimension of the previous cube in the path (`test.test2`, where
// `test` has both a `test2` dimension and a join to cube `test2`) fails to compile
// with "test.test2.name cannot be resolved".
describe('Issue #10371 - join_path cube name collides with dimension name', () => {
  const model = `
cubes:
  - name: test
    sql_table: test
    dimensions:
      - name: id
        type: string
        sql: id
        primary_key: true
      - name: test2
        type: string
        sql: test2
    joins:
      - name: test2
        relationship: many_to_one
        sql: "{CUBE.test2} = {test2.id}"

  - name: test2
    sql_table: test2
    dimensions:
      - name: id
        type: string
        sql: id
        primary_key: true
      - name: name
        type: string
        sql: name

views:
  - name: v_test
    cubes:
      - join_path: test
        prefix: true
        includes:
          - id
      - join_path: test.test2
        prefix: true
        includes:
          - name
`;

  it('compiles the view and resolves the joined cube member', async () => {
    const { compiler, joinGraph, cubeEvaluator, metaTransformer } = prepareYamlCompiler(model);
    await compiler.compile();
    compiler.throwIfAnyErrors();

    const view = metaTransformer.cubes.map((c: any) => c.config).find((c: any) => c.name === 'v_test');
    const dims = view.dimensions.map((d: any) => d.name).sort();
    expect(dims).toEqual(['v_test.test2_name', 'v_test.test_id']);

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      dimensions: ['v_test.test2_name'],
    });
    const [sql] = query.buildSqlAndParams();
    expect(sql).toContain('"test2".name');
  });
});
