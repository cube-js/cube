import { prepareCompiler as originalPrepareCompiler } from '@cubejs-backend/schema-compiler';

import { DruidQuery } from '../src/DruidQuery';

export const testCompiler = (content, options) => originalPrepareCompiler({
  localPath: () => __dirname,
  dataSchemaFiles: () => Promise.resolve([
    { fileName: 'main.js', content },
  ]),
}, { adapter: 'druid', ...options });

describe('DruidQuery', () => {
  const { compiler, joinGraph, cubeEvaluator } = testCompiler(`
    cube(\`visitors\`, {
      sql: \`
      select * from visitors
      \`,

      measures: {
        count: {
          type: 'count'
        }
      },

      dimensions: {
        name: {
          type: 'string',
          sql: 'name'
        },
        createdAt: {
            sql: \`created_at\`,
            type: 'time',
        }
      }

    })
    `, {});

  const likeQuery = (operator: string, value: string) => new DruidQuery(
    { joinGraph, cubeEvaluator, compiler },
    {
      measures: [],
      filters: [
        {
          member: 'visitors.name',
          operator,
          values: [value],
        },
      ],
    },
  );

  it('druid query like test',
    () => compiler.compile().then(() => {
      const queryAndParams = likeQuery('contains', 'demo').buildSqlAndParams();
      expect(queryAndParams[0]).toContain('LIKE CONCAT(\'%\', LOWER(?), \'%\') ESCAPE \'\\\')');
    }));

  // Druid SQL is Calcite-based: without the ESCAPE clause above there is no escape character at
  // all, so the `\` BaseFilter.escapeWildcardChars binds would stay a literal backslash
  it('druid query escapes LIKE wildcards in filter parameters',
    () => compiler.compile().then(() => {
      expect(likeQuery('contains', 'a_b%').buildSqlAndParams()[1]).toEqual(['a\\_b\\%']);
      expect(likeQuery('startsWith', '100%').buildSqlAndParams()[1]).toEqual(['100\\%']);
      expect(likeQuery('endsWith', 'c:\\users').buildSqlAndParams()[1]).toEqual(['c:\\\\users']);
    }));

  it('druid query renders an ESCAPE clause for the native planner too',
    () => compiler.compile().then(() => {
      const templates = likeQuery('contains', 'demo').sqlTemplates();
      expect(templates.filters.like_escape_char).toEqual('\\');
      expect(templates.filters.like_pattern).toContain('ESCAPE \'\\\'');
      // Druid has no ILIKE, LOWER() on both sides stands in for it
      expect(templates.tesseract.ilike).toEqual(
        'LOWER({{ expr }}) {% if negated %}NOT {% endif %}LIKE {{ pattern }}'
      );
    }));

  it('druid query timezone shift test', () => compiler.compile().then(() => {
    const query = new DruidQuery(
      { joinGraph, cubeEvaluator, compiler },
      {
        timeDimensions: [
          {
            dimension: 'visitors.createdAt',
            granularity: 'day',
          },
        ],
        measures: [],
        timezone: 'Europe/Kiev',
      },
    );
    const queryAndParams = query.buildSqlAndParams();
    expect(queryAndParams[0]).toContain('CAST(TIME_FORMAT("visitors".created_at, \'yyyy-MM-dd HH:mm:ss\', \'Europe/Kiev\') AS TIMESTAMP)');
  }));
});
