/* eslint-disable quotes */
import { prepareCompiler as originalPrepareCompiler } from '@cubejs-backend/schema-compiler';

import { PinotQuery } from '../../src/PinotQuery';

const prepareCompiler = (content: string) => originalPrepareCompiler({
  localPath: () => __dirname,
  dataSchemaFiles: () => Promise.resolve([{ fileName: 'main.js', content }]),
});

// PinotFilter has carried `ESCAPE '\'` all along; the native planner declares the same escape
// character through `filters.like_escape_char` but had no clause to attach it to.
describe('PinotQuery', () => {
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

  const likeQuery = (operator: string, value: string) => new PinotQuery(
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

  it('escapes LIKE wildcards in filter parameters', async () => {
    await compiler.compile();

    const [sql, params] = likeQuery('contains', 'a_b%').buildSqlAndParams();

    expect(sql).toContain("LIKE CONCAT('%', LOWER(?) , '%') ESCAPE '\\'");
    expect(params).toEqual(['a\\_b\\%']);
    expect(likeQuery('startsWith', '100%').buildSqlAndParams()[1]).toEqual(['100\\%']);
    expect(likeQuery('endsWith', 'c:\\users').buildSqlAndParams()[1]).toEqual(['c:\\\\users']);
  });

  it('renders an ESCAPE clause for the native planner too', async () => {
    await compiler.compile();

    const templates = likeQuery('contains', 'demo').sqlTemplates();

    expect(templates.filters.like_escape_char).toEqual('\\');
    expect(templates.filters.like_pattern).toContain("ESCAPE '\\'");
    // Pinot has no ILIKE, LOWER() on both sides stands in for it
    expect(templates.tesseract.ilike).toEqual(
      'LOWER({{ expr }}) {% if negated %}NOT {% endif %}LIKE {{ pattern }}'
    );
  });
});
