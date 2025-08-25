import { PostgresQuery } from '../../src/adapter/PostgresQuery';
import { prepareCompiler } from './PrepareCompiler';

describe('JavaScript SQL Comments Preservation', () => {
  it('preserves SQL comments in JS models', async () => {
    const { compiler, joinGraph, cubeEvaluator } = prepareCompiler([
      {
        fileName: 'test.js',
        content: `
          cube('JSTestCube', {
            sql: \`
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
              FROM \\\`table\\\`.\\\`record\\\` r
              JOIN \\\`table\\\`.\\\`object\\\` o ON r.object_id = o.id
              -- Get company name
              LEFT JOIN \\\`table\\\`.\\\`record_value\\\` company_name ON r.id = company_name.record_id
                  AND company_name.name = 'name'
              WHERE r._fivetran_deleted = FALSE
                  AND o.singular_noun = 'Company'
            \`,

            dimensions: {
              record_id: {
                sql: 'record_id',
                type: 'string',
                primaryKey: true
              }
            },

            measures: {
              count: {
                type: 'count'
              }
            }
          });
        `
      }
    ]);

    await compiler.compile();

    // Build a simple query to extract the actual SQL
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: ['JSTestCube.count'],
      dimensions: ['JSTestCube.record_id'],
      timezone: 'UTC'
    });

    const [sql] = query.buildSqlAndParams();

    // Verify that SQL comments are preserved on separate lines
    expect(sql).toContain('-- Extract target_record_id from workspace association JSON');
    expect(sql).toContain('-- Get actual workspace name by joining with workspace record');
    expect(sql).toContain('-- Get company name');

    // Ensure comments are on separate lines in JS models
    const lines = sql.split('\n');
    const commentLine = lines.find(line => line.trim() === '-- Get company name');
    expect(commentLine).toBeDefined();
  });

  it('handles edge cases in JS SQL strings', async () => {
    const { compiler, joinGraph, cubeEvaluator } = prepareCompiler([
      {
        fileName: 'edge-cases.js',
        content: `
          cube('EdgeCasesTest', {
            sql: \`
              SELECT
                  id,
                  -- Comment with 'quotes' and "double quotes"
                  name,
                  -- Comment with special chars: !@#$%^&*()
                  email,
                  created_at
              FROM users
              -- SQL string in comment: SELECT * FROM table
              WHERE active = true
            \`,

            dimensions: {
              id: {
                sql: 'id',
                type: 'string',
                primaryKey: true
              }
            },

            measures: {
              count: {
                type: 'count'
              }
            }
          });
        `
      }
    ]);

    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: ['EdgeCasesTest.count'],
      dimensions: ['EdgeCasesTest.id'],
      timezone: 'UTC'
    });

    const [sql] = query.buildSqlAndParams();

    const testLines = [
      '-- Comment with \'quotes\' and "double quotes"',
      '-- Comment with special chars: !@#$%^&*()',
      '-- SQL string in comment: SELECT * FROM table',
    ];

    // Ensure all comments are properly preserved
    const lines = sql.split('\n').map(l => l.trim());
    for (const testLine of testLines) {
      expect(lines.includes(testLine)).toBeTruthy();
    }
  });
});
