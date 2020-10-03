/* globals it, describe, after */
/* eslint-disable quote-props */
const MssqlQuery = require('../../adapter/MssqlQuery');
const PrepareCompiler = require('./PrepareCompiler');
require('should');

const { prepareCompiler } = PrepareCompiler;

describe('MssqlQuery', () => {
  const { compiler, joinGraph, cubeEvaluator } = prepareCompiler(`
    cube(\`visitors\`, {
      sql: \`
      select * from visitors
      \`,

      measures: {
        count: {
          type: 'count'
        },

        unboundedCount: {
          type: 'count',
          rollingWindow: {
            trailing: 'unbounded'
          }
        }
      },

      dimensions: {
        createdAt: {
          type: 'time',
          sql: 'created_at'
        }
      }
    });
    `);

  it('group by the date_from field on unbounded trailing windows',
    () => compiler.compile().then(() => {
      const query = new MssqlQuery(
        { joinGraph, cubeEvaluator, compiler },
        {
          measures: ['visitors.count', 'visitors.unboundedCount'],
          timeDimensions: [
            {
              dimension: 'visitors.createdAt',
              granularity: 'week',
              dateRange: ['2017-01-01', '2017-01-30'],
            },
          ],
          timezone: 'America/Los_Angeles',
          order: [
            {
              id: 'visitors.createdAt',
            },
          ],
        }
      );

      const queryAndParams = query.buildSqlAndParams();

      const queryString = queryAndParams[0];
      const lastGroupByIdx = queryString.lastIndexOf('GROUP BY');
      const queryCloseIdx = queryString.indexOf(')', lastGroupByIdx + 1);
      const finalGroupBy = queryString.substring(lastGroupByIdx, queryCloseIdx);

      finalGroupBy.should.equal('GROUP BY "visitors.createdAt_series"."date_from"');
    }));
});
