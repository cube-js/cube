import { prepareYamlCompiler } from './PrepareCompiler';

describe('number_agg measure without multi_stage (#12024)', () => {
  const schema = (sql: string) => `
cubes:
  - name: orders
    sql_table: orders
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: status
        sql: status
        type: string
    measures:
      - name: max_amount_agg
        sql: "${sql}"
        type: number_agg
`;

  it.each([
    ['MAX({CUBE}.amount)'],
    ['amount'],
  ])('compiles with sql: %s', async (sql) => {
    const { compiler } = prepareYamlCompiler(schema(sql));
    await compiler.compile();
  });
});
