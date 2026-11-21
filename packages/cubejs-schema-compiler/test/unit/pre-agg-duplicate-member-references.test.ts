import { prepareYamlCompiler } from './PrepareCompiler';

// https://github.com/cube-js/cube/issues/3486
// A rollup that lists the same member twice compiles today, but its build fails
// in Cube Store with "Create table failed: Duplicate column 'orders__status' in CSV header".
// The data model compiler should reject such a pre-aggregation up front.
describe('Pre-aggregation with non-unique member references', () => {
  const schema = (measures: string[], dimensions: string[]) => `
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
      - name: created_at
        sql: created_at
        type: time
    measures:
      - name: count
        type: count
      - name: amount
        sql: amount
        type: sum
    pre_aggregations:
      - name: dup
        measures: [${measures.join(', ')}]
        dimensions: [${dimensions.join(', ')}]
        time_dimension: CUBE.created_at
        granularity: day
`;

  it('rejects a dimension referenced twice', async () => {
    const compilers = prepareYamlCompiler(schema(['CUBE.count'], ['CUBE.status', 'CUBE.status']));
    await expect(compilers.compile()).rejects.toThrow(/status/);
  });

  it('rejects a measure referenced twice', async () => {
    const compilers = prepareYamlCompiler(schema(['CUBE.count', 'CUBE.amount', 'CUBE.count'], ['CUBE.status']));
    await expect(compilers.compile()).rejects.toThrow(/count/);
  });

  it('accepts unique member references', async () => {
    const compilers = prepareYamlCompiler(schema(['CUBE.count', 'CUBE.amount'], ['CUBE.status']));
    await compilers.compile();
  });
});
