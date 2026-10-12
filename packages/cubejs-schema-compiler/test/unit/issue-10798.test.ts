import { prepareYamlCompiler } from './PrepareCompiler';

// https://github.com/cube-js/cube/issues/10798
// YAML models use snake_case measure types (`count_distinct`, `number_agg`), but an
// invalid type is reported with the camelized value and a camelCase list of allowed
// types, e.g. `type = numberAggr) must be one of [..., countDistinct, ..., numberAgg, rank]`.
describe('issue-10798: invalid measure type error lists values as written in YAML', () => {
  it('reports snake_case values for a YAML model', async () => {
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
      - name: total
        sql: amount
        type: number_aggr
`);

    let message = '';

    try {
      await compiler.compile();
    } catch (e: any) {
      message = e.message;
    }

    expect(message).toContain('must be one of');
    // The invalid value should be echoed as the user wrote it
    expect(message).toContain('number_aggr');
    expect(message).not.toContain('numberAggr');
    // Allowed values should be in the YAML (snake_case) spelling
    expect(message).toContain('count_distinct');
    expect(message).not.toContain('countDistinct');
  });
});
