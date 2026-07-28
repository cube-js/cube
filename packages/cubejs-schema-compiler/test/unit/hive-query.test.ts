/* eslint-disable no-restricted-syntax, quotes */
import { HiveQuery } from '../../src/adapter/HiveQuery';
import { prepareJsCompiler } from './PrepareCompiler';

describe('HiveQuery', () => {
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

  const containsParams = async (value: string) => {
    await compiler.compile();

    const query = new HiveQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: ['visitors.count'],
      filters: [
        {
          member: 'visitors.source',
          operator: 'contains',
          values: [value],
        },
      ],
      timezone: 'UTC',
    });

    const [sql, params] = query.buildSqlAndParams();

    expect(sql).toContain("LIKE CONCAT('%', ?, '%')");

    return params;
  };

  it('escapes LIKE wildcards in filter parameters', async () => {
    expect(await containsParams('a_b%')).toEqual(['a\\_b\\%']);
  });

  // HiveQL's LIKE takes no ESCAPE clause and Hive does not collapse `\\` back to a single
  // backslash, so escaping it would turn `c:\users` into an unmatchable `c:\\users`
  it('leaves literal backslashes alone, Hive has no ESCAPE clause', async () => {
    expect(await containsParams('c:\\users')).toEqual(['c:\\users']);
  });
});
