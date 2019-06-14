const MongoBiQuery = require('../adapter/MongoBiQuery');
const PrepareCompiler = require('./PrepareCompiler');
require('should');

const { prepareCompiler } = PrepareCompiler;

describe('MongoBiQuery', () => {
  const { compiler, joinGraph, cubeEvaluator } = prepareCompiler(`
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
        createdAt: {
          type: 'time',
          sql: 'created_at'
        }
      }
    });
    `);

  it('convert_tz implementation', () => {
    return compiler.compile().then(() => {
      const query = new MongoBiQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: [
          'visitors.count'
        ],
        timeDimensions: [{
          dimension: 'visitors.createdAt',
          granularity: 'date',
          dateRange: ['2017-01-01', '2017-01-30']
        }],
        timezone: 'America/Los_Angeles',
        order: [{
          id: 'visitors.createdAt'
        }]
      });

      const queryAndParams = query.buildSqlAndParams();
      console.log(queryAndParams);
      queryAndParams[0].should.match(/TIMESTAMPADD\(HOUR, -7/);
    });
  });
});
