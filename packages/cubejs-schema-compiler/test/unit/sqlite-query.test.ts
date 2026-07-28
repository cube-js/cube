/* eslint-disable no-restricted-syntax, quotes */
import { SqliteQuery } from '../../src/adapter/SqliteQuery';
import { prepareJsCompiler } from './PrepareCompiler';

describe('SqliteQuery', () => {
  const { compiler, joinGraph, cubeEvaluator } = prepareJsCompiler(`
    cube('visitors', {
      sql: \`SELECT * FROM visitors\`,
      dimensions: {
        id: {
          sql: \`id\`,
          type: 'number',
          primaryKey: true
        },
        source: {
          sql: \`source\`,
          type: 'string'
        }
      },
      measures: {
        count: {
          type: 'count',
        }
      }
    });
    `);

  it('emits ESCAPE for LIKE filters, SQLite has no default escape character', async () => {
    await compiler.compile();

    const query = new SqliteQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: ['visitors.count'],
      filters: [
        {
          member: 'visitors.source',
          operator: 'contains',
          values: ['folder\\name_%'],
        },
      ],
      timezone: 'UTC',
    });

    const [sql, params] = query.buildSqlAndParams();

    // ESCAPE binds to the whole right operand, so it must come after COLLATE NOCASE
    expect(sql).toContain("LIKE '%' || ? || '%' COLLATE NOCASE ESCAPE '\\'");
    expect(params).toEqual(['folder\\\\name\\_\\%']);
  });
});
