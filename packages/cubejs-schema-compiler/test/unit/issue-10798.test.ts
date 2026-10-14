import { prepareYamlCompiler } from './PrepareCompiler';

// https://github.com/cube-js/cube/issues/10798
// YAML models use snake_case type names, so the validation error must list them
// in snake_case too, not as the internal camelCase enum values.
describe('Invalid measure type error message (#10798)', () => {
  it('invalid measure type error lists the YAML (snake_case) type names', async () => {
    const { compiler } = prepareYamlCompiler(`
cubes:
  - name: orders
    sql_table: orders
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
    measures:
      - name: amount
        sql: amount
        type: number_aggr
`);

    let message = '';

    try {
      await compiler.compile();
    } catch (e: any) {
      message = e.toString();
    }

    expect(message).toMatch(/must be one of/);
    expect(message).toContain('count_distinct');
    expect(message).toContain('number_agg');
    expect(message).not.toContain('countDistinct');
    expect(message).not.toContain('numberAgg');
  });
});
