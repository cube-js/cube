/* eslint-disable quote-props */
/* globals describe, it, after */
const PostgresQuery = require('../adapter/PostgresQuery');
const PrepareCompiler = require('./PrepareCompiler');
require('should');

const { prepareCompiler } = PrepareCompiler;
const dbRunner = require('./DbRunner');

describe('AsyncModule', function test() {
  this.timeout(20000);

  after(async () => {
    await dbRunner.tearDown();
  });

  it('gutter', () => {
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
    return compiler.compile().then(() => {
      const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: ['visitors.visitor_count'],
        timezone: 'America/Los_Angeles'
      });

      console.log(query.buildSqlAndParams());
      return dbRunner.testQuery(query.buildSqlAndParams()).then(res => {
        res.should.be.deepEqual(
          [
            { "visitors__visitor_count": "6" }
          ]
        );
      });
    });
  });

  it('import local node module', () => {
    const { joinGraph, cubeEvaluator, compiler } = prepareCompiler(`
    import { foo } from '../test/TestHelperForImport.js';
    
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
    return compiler.compile().then(() => {
      const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: ['bar.visitor_count'],
        timezone: 'America/Los_Angeles'
      });

      console.log(query.buildSqlAndParams());
      return dbRunner.testQuery(query.buildSqlAndParams()).then(res => {
        res.should.be.deepEqual(
          [
            { "bar__visitor_count": "6" }
          ]
        );
      });
    });
  });
});
