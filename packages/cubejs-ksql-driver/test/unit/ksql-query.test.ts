/* eslint-disable quotes */
import { prepareCompiler as originalPrepareCompiler } from '@cubejs-backend/schema-compiler';

import { KsqlQuery } from '../../src/KsqlQuery';

const prepareCompiler = (content: string) => originalPrepareCompiler({
  localPath: () => __dirname,
  dataSchemaFiles: () => Promise.resolve([{ fileName: 'main.js', content }]),
});

// ksqlDB spells the predicate `NOT? LIKE pattern (ESCAPE escape=STRING)?` and has no default
// escape character, so the `\` BaseFilter.escapeWildcardChars binds is only an escape character
// when the ESCAPE clause is there.
describe('KsqlQuery', () => {
  const { compiler, joinGraph, cubeEvaluator } = prepareCompiler(`
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

  const likeQuery = (operator: string, value: string) => new KsqlQuery(
    { joinGraph, cubeEvaluator, compiler },
    {
      measures: ['visitors.count'],
      filters: [
        {
          member: 'visitors.source',
          operator,
          values: [value],
        },
      ],
      timezone: 'UTC',
    },
  );

  it('renders an ESCAPE clause', async () => {
    await compiler.compile();

    const [sql, params] = likeQuery('contains', 'demo').buildSqlAndParams();

    expect(sql).toContain("ILIKE CONCAT('%', ?, '%') ESCAPE '\\'");
    expect(params).toEqual(['demo']);
  });

  it('escapes LIKE wildcards in filter parameters', async () => {
    await compiler.compile();

    expect(likeQuery('contains', 'a_b%').buildSqlAndParams()[1]).toEqual(['a\\_b\\%']);
    expect(likeQuery('startsWith', '100%').buildSqlAndParams()[1]).toEqual(['100\\%']);
    expect(likeQuery('endsWith', 'c:\\users').buildSqlAndParams()[1]).toEqual(['c:\\\\users']);
  });

  it('renders an ESCAPE clause for the native planner too', async () => {
    await compiler.compile();

    const templates = likeQuery('contains', 'demo').sqlTemplates();

    expect(templates.filters.like_escape_char).toEqual('\\');
    expect(templates.filters.like_pattern).toContain("ESCAPE '\\'");
  });
});
