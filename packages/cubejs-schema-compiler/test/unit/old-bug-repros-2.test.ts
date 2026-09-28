import { prepareJsCompiler, prepareYamlCompiler } from './PrepareCompiler';

// Repros for old open GitHub issues. Each test asserts the correct behavior,
// so it fails until its bug is fixed.
describe('old bug repros (unit)', () => {
  describe('issue #10371 cube name in join_path collides with a dimension name', () => {
    it('compiles a view that joins through `test.test2`', async () => {
      const { compiler, metaTransformer } = prepareYamlCompiler(`
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
`);
      await compiler.compile();
      const view = metaTransformer.cubes.find((c: any) => c.config.name === 'v_test');
      expect(view?.config.dimensions.map((d: any) => d.name).sort()).toEqual(['v_test.test2_name', 'v_test.test_id']);
    });
  });

  describe('issue #3486 pre-aggregation with non-unique member references', () => {
    it('rejects a rollup that references the same dimension twice', async () => {
      const { compiler } = prepareJsCompiler(`
        cube('orders', {
          sql: 'SELECT 1 AS id, \\'a\\' AS status',
          measures: { count: { type: 'count' } },
          dimensions: {
            id: { sql: 'id', type: 'number', primaryKey: true },
            status: { sql: 'status', type: 'string' },
          },
          preAggregations: {
            main: {
              measures: [CUBE.count],
              dimensions: [CUBE.status, CUBE.status],
            },
          },
        });
      `);
      await expect(compiler.compile()).rejects.toThrow(/status/);
    });
  });
});
