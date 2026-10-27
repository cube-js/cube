import { MongoBiQuery } from '../../src/adapter/MongoBiQuery';
import { prepareJsCompiler } from './PrepareCompiler';

describe('MongoBiQuery', () => {
  const compilers = prepareJsCompiler(`
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

  it('convert_tz implementation', () => compilers.compile().then(() => {
    const query = compilers.createQuery(MongoBiQuery, {
      measures: [
        'visitors.count'
      ],
      timeDimensions: [{
        dimension: 'visitors.createdAt',
        granularity: 'day',
        dateRange: ['2017-01-01', '2017-01-30']
      }],
      timezone: 'America/Los_Angeles',
      order: [{
        id: 'visitors.createdAt'
      }]
    });

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);
    expect(queryAndParams[0]).toMatch(/TIMESTAMPADD\(HOUR, -/);
  }));
});
