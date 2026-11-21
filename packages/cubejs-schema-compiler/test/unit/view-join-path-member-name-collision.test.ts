import { prepareYamlCompiler } from './PrepareCompiler';

// https://github.com/cube-js/cube/issues/10371
// A join path segment names a joined cube that shares its name with a dimension
// of the previous cube in the path: `test.test2` must resolve to the joined
// cube `test2`, not to the dimension `test.test2`.
describe('View join_path segment colliding with a dimension name', () => {
  it('compiles a view whose join_path cube name equals a dimension of the parent cube', async () => {
    const compilers = prepareYamlCompiler(`
cubes:
  - name: test
    sql_table: test
    dimensions:
      - name: id
        sql: id
        type: string
        primary_key: true
      - name: test2
        sql: test2
        type: string
    joins:
      - name: test2
        relationship: many_to_one
        sql: "{CUBE.test2} = {test2.id}"

  - name: test2
    sql_table: test2
    dimensions:
      - name: id
        sql: id
        type: string
        primary_key: true
      - name: name
        sql: name
        type: string

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
`);

    // Fails today with: "test.test2.name cannot be resolved. There's no such member or cube."
    await compilers.compile();

    const viewDef = compilers.cubeEvaluator.getCubeDefinition('v_test');
    expect(Object.keys(viewDef.dimensions!).sort()).toEqual(['test2_name', 'test_id']);
  });
});
