import { PostgresQuery } from '../../src/adapter/PostgresQuery';
import { prepareYamlCompiler } from './PrepareCompiler';

describe('YAML SQL Formatting Preservation', () => {
  it('handles sql: > (folded scalar)', async () => {
    const { compiler, joinGraph, cubeEvaluator } = prepareYamlCompiler(
      `
      cubes:
      - name: Orders
        sql:     >
          SELECT
              r.id as record_id,
              r.created_at as record_created_at,
              -- Extract target_record_id from workspace association JSON
              JSON_EXTRACT_SCALAR(workspace.value, '$[0].target_record_id') as workspace_target_record_id,
              -- Get actual workspace name by joining with workspace record
              CASE
                  WHEN workspace_name.value IS NOT NULL
                  THEN JSON_EXTRACT_SCALAR(JSON_EXTRACT_ARRAY(workspace_name.value)[OFFSET(0)], '$.value')
                  ELSE NULL
              END as workspace_name
          FROM \`table\`.\`record\` r
          JOIN \`table\`.\`object\` o ON r.object_id = o.id
          -- Get company name
          LEFT JOIN \`table\`.\`record_value\` company_name ON r.id = company_name.record_id
              AND company_name.name = 'name'
          WHERE r._fivetran_deleted = FALSE
              AND o.singular_noun = 'Company'

        dimensions:
          - name: record_id
            sql: record_id
            type: string
            primaryKey: true
        measures:
          - name: count
            type: count
      `
    );

    await compiler.compile();

    // Build a simple query to extract the actual SQL
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: ['Orders.count'],
      dimensions: ['Orders.record_id'],
      timezone: 'UTC'
    });

    const [sql] = query.buildSqlAndParams();

    // Verify that SQL comments are preserved on separate lines
    expect(sql).toContain('-- Extract target_record_id from workspace association JSON');
    expect(sql).toContain('-- Get actual workspace name by joining with workspace record');
    expect(sql).toContain('-- Get company name');

    // Most importantly, ensure comments are NOT merged with the previous line
    const lines = sql.split('\n');
    const commentLine = lines.find(line => line.trim() === '-- Get company name');
    expect(commentLine).toBeDefined();
  });

  it('handles sql: | (literal scalar)', async () => {
    const { compiler, joinGraph, cubeEvaluator } = prepareYamlCompiler(
      `
      cubes:
      - name: TestCube
        sql: |
          SELECT id, name
          -- Comment here
          FROM table1
          WHERE active = true

        dimensions:
          - name: id
            sql: id
            type: string
            primaryKey: true
        measures:
          - name: count
            type: count
      `
    );

    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: ['TestCube.count'],
      dimensions: ['TestCube.id'],
      timezone: 'UTC'
    });

    const [sql] = query.buildSqlAndParams();

    // Should preserve comments with literal scalar
    expect(sql).toContain('-- Comment here');
    const lines = sql.split('\n');
    const commentLine = lines.find(line => line.trim() === '-- Comment here');
    expect(commentLine).toBeDefined();
  });

  it('handles single-line SQL without multilines', async () => {
    const { compiler, joinGraph, cubeEvaluator } = prepareYamlCompiler(
      `
      cubes:
      - name: TestCube
        sql: "SELECT id, name FROM table1"

        dimensions:
          - name: id
            sql: id
            type: string
            primaryKey: true
        measures:
          - name: count
            type: count
      `
    );

    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: ['TestCube.count'],
      dimensions: ['TestCube.id'],
      timezone: 'UTC'
    });

    const [sql] = query.buildSqlAndParams();

    // Should work normally for single-line SQL
    expect(sql).toContain('SELECT id, name FROM table1');
  });

  it('works correctly for SQL without comments', async () => {
    const { compiler, joinGraph, cubeEvaluator } = prepareYamlCompiler(
      `
      cubes:
      - name: SimpleOrders
        sql: >
          SELECT
              id,
              amount,
              status
          FROM orders
          WHERE active = true

        dimensions:
          - name: id
            sql: id
            type: string
            primaryKey: true
        measures:
          - name: count
            type: count
      `
    );

    await compiler.compile();

    // Build a simple query to extract the actual SQL
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: ['SimpleOrders.count'],
      dimensions: ['SimpleOrders.id'],
      timezone: 'UTC'
    });

    const [sql] = query.buildSqlAndParams();

    // Should still work normally for SQL without comments
    expect(sql).toContain('SELECT');
    expect(sql).toContain('FROM orders');
    expect(sql).toContain('WHERE active = true');
  });
});
