/* globals describe, it */
const PostgresQuery = require('../adapter/PostgresQuery');
const PrepareCompiler = require('./PrepareCompiler');
require('should');

const { prepareCompiler } = PrepareCompiler;
const dbRunner = require('./DbRunner');

describe('AsyncModule', () => {
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
            { "visitors.visitor_count": "6" }
          ]
        );
      });
    });
  });
});
