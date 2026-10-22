import { prepareCompiler as originalPrepareCompiler } from '@cubejs-backend/schema-compiler';

import { DruidQuery } from '../src/DruidQuery';

export const testCompiler = (content, options) => originalPrepareCompiler({
  localPath: () => __dirname,
  dataSchemaFiles: () => Promise.resolve([
    { fileName: 'main.js', content },
  ]),
}, { adapter: 'druid', ...options });

describe('DruidQuery', () => {
  const compilers = testCompiler(`
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

  it('druid query like test',
    () => compilers.compile().then(() => {
      const query = compilers.createQuery(DruidQuery, {
        measures: [],
        filters: [
          {
            member: 'visitors.name',
            operator: 'contains',
            values: [
              'demo',
            ],
          },
        ],
      });
      const queryAndParams = query.buildSqlAndParams();
      expect(queryAndParams[0]).toContain('LIKE CONCAT(\'%\', LOWER(?), \'%\') ESCAPE \'\\\'');
    }));

  it('druid query timezone shift test', () => compilers.compile().then(() => {
    const query = compilers.createQuery(DruidQuery, {
      timeDimensions: [
        {
          dimension: 'visitors.createdAt',
          granularity: 'day',
        },
      ],
      measures: [],
      timezone: 'Europe/Kiev',
    });
    const queryAndParams = query.buildSqlAndParams();
    expect(queryAndParams[0]).toContain('CAST(TIME_FORMAT("visitors".created_at, \'yyyy-MM-dd HH:mm:ss\', \'Europe/Kiev\') AS TIMESTAMP)');
  }));

  // A rolling window, a shifted date range and a custom granularity offset all reach the dialect
  // through these two, and only the sign tells them apart.
  it('subtracts and adds an interval in opposite directions', () => {
    const query = compilers.createQuery(DruidQuery, {});

    expect(query.subtractInterval('d', '7 day')).toEqual('(d - INTERVAL 7 day)');
    expect(query.addInterval('d', '7 day')).toEqual('(d + INTERVAL 7 day)');
  });
});
