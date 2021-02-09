import { PostgresQuery } from '../../../src/adapter/PostgresQuery';
import { prepareCompiler } from '../../unit/PrepareCompiler';

import { PostgresDBRunner } from './PostgresDBRunner';

describe('AsyncModule', () => {
  jest.setTimeout(200000);

  const dbRunner = new PostgresDBRunner();

  afterAll(async () => {
    await dbRunner.tearDown();
  });

  it('gutter', async () => {
    const { joinGraph, cubeEvaluator, compiler } = prepareCompiler(`
    const rp = require('request-promise');
    
    asyncModule(async () => {
      await rp('http://www.google.com');
      
      cube('visitors', {
        sql: \`
        select * from visitors
        \`,
  
        measures: {
          visitor_count: {
            type: 'count',
            sql: 'id',
            drillMembers: [source, created_at]
          },
          visitor_revenue: {
            type: 'sum',
            sql: 'amount',
            drillMemberReferences: [source, created_at]
          }
        },
  
        dimensions: {
          source: {
            type: 'string',
            sql: 'source'
          },
          created_at: {
            type: 'time',
            sql: 'created_at'
          }
        }
      })
    })
    `, { allowNodeRequire: true });
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: ['visitors.visitor_count'],
      timezone: 'America/Los_Angeles'
    });

    console.log(query.buildSqlAndParams());
    expect(await dbRunner.testQuery(query.buildSqlAndParams())).toEqual([
      { visitors__visitor_count: '6' }
    ]);
  });

  it('import local node module', async () => {
    const { joinGraph, cubeEvaluator, compiler } = prepareCompiler(`
    import { foo } from '../../test/unit/TestHelperForImport.js';
    
    cube(foo(), {
      sql: \`
      select * from visitors
      \`,

      measures: {
        visitor_count: {
          type: 'count',
          sql: 'id'
        },
        visitor_revenue: {
          type: 'sum',
          sql: 'amount'
        }
      },

      dimensions: {
        source: {
          type: 'string',
          sql: 'source'
        },
        created_at: {
          type: 'time',
          sql: 'created_at'
        }
      }
    })
    `, { allowNodeRequire: true });

    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: ['bar.visitor_count'],
      timezone: 'America/Los_Angeles'
    });

    console.log(query.buildSqlAndParams());
    expect(await dbRunner.testQuery(query.buildSqlAndParams())).toEqual([
      { bar__visitor_count: '6' }
    ]);
  });
});
