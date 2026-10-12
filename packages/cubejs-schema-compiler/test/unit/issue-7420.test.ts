import { prepareYamlCompiler } from './PrepareCompiler';
import { PostgresQuery } from '../../src/adapter/PostgresQuery';

// https://github.com/cube-js/cube/issues/7420
// Index on an `original_sql` pre-aggregation references the cube-member alias
// (`base_positions__position_id`) instead of the actual column of the table
// built from the cube's SQL (`position_id`).
describe('Issue #7420: index for original_sql pre-aggregation', () => {
  const schema = `
cubes:
  - name: base_positions
    sql: SELECT 'id_1' AS position_id, 'test' AS position_name
    measures:
      - name: count
        type: count
    dimensions:
      - name: position_id
        sql: position_id
        type: string
        primary_key: true
      - name: position_name
        sql: position_name
        type: string
    pre_aggregations:
      - name: main
        type: original_sql
        external: false
        indexes:
          - name: position_id_index
            columns:
              - position_id
`;

  it('creates index on the original column name', async () => {
    const { compiler, cubeEvaluator, joinGraph } = prepareYamlCompiler(schema);
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      dimensions: ['base_positions.position_name'],
      measures: ['base_positions.count'],
    });

    const description: any = query.preAggregations?.preAggregationsDescription();
    const main = description.find(d => d.preAggregationId === 'base_positions.main');
    expect(main).toBeDefined();
    expect(main.indexesSql[0].sql[0]).toContain('("position_id")');
    expect(main.indexesSql[0].sql[0]).not.toContain('base_positions__position_id');
    expect(main.createTableIndexes[0].columns).toEqual(['"position_id"']);
  });
});
