/* globals describe,it,after */
/* eslint-disable quote-props */
const R = require('ramda');
require('should');

const MySqlQuery = require('../adapter/MysqlQuery');
const { prepareCompiler } = require('./PrepareCompiler');

const dbRunner = require('./MySqlDbRunner');

describe('MySqlPreAggregations', function test() {
  this.timeout(30000);

  after(async () => {
    await dbRunner.tearDown();
  });

  const { compiler, joinGraph, cubeEvaluator } = prepareCompiler(`
    cube(\`visitors\`, {
      sql: \`
      select * from visitors
      \`,

      measures: {
        count: {
          type: 'count'
        },
        
        checkinsTotal: {
          sql: \`\${checkinsCount}\`,
          type: 'sum'
        },
        
        uniqueSourceCount: {
          sql: 'source',
          type: 'countDistinct'
        },
        
        countDistinctApprox: {
          sql: 'id',
          type: 'countDistinctApprox'
        },
        
        ratio: {
          sql: \`1.0 * \${uniqueSourceCount} / nullif(\${checkinsTotal}, 0)\`,
          type: 'number'
        }
      },

      dimensions: {
        id: {
          type: 'number',
          sql: 'id',
          primaryKey: true
        },
        source: {
          type: 'string',
          sql: 'source'
        },
        createdAt: {
          type: 'time',
          sql: 'created_at'
        }
      },
      
      segments: {
        google: {
          sql: \`source = 'google'\`
        }
      },
      
      preAggregations: {
        partitioned: {
          type: 'rollup',
          measureReferences: [count],
          dimensionReferences: [source],
          timeDimensionReference: createdAt,
          granularity: 'day',
          partitionGranularity: 'month'
        }
      }
    })
    `);

  function replaceTableName(query, preAggregation, suffix) {
    const [toReplace, params] = query;
    console.log(toReplace);
    preAggregation = Array.isArray(preAggregation) ? preAggregation : [preAggregation];
    return [
      preAggregation.reduce((replacedQuery, desc) =>
        replacedQuery.replace(new RegExp(desc.tableName, 'g'), desc.tableName + '_' + suffix), toReplace
      ),
      params
    ];
  }

  function tempTablePreAggregations(preAggregationsDescriptions) {
    return R.unnest(preAggregationsDescriptions.map(desc =>
      desc.invalidateKeyQueries.concat([
        [desc.loadSql[0].replace('CREATE TABLE', 'CREATE TEMPORARY TABLE'), desc.loadSql[1]]
      ])
    ));
  }

  it('partitioned', () => {
    return compiler.compile().then(() => {
      const query = new MySqlQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: [
          'visitors.count'
        ],
        dimensions: [
          'visitors.source'
        ],
        timezone: 'America/Los_Angeles',
        preAggregationsSchema: '',
        timeDimensions: [{
          dimension: 'visitors.createdAt',
          granularity: 'day',
          dateRange: ['2016-12-30', '2017-01-05']
        }],
        order: [{
          id: 'visitors.createdAt'
        }],
      });

      const queryAndParams = query.buildSqlAndParams();
      console.log(queryAndParams);
      const preAggregationsDescription = query.preAggregations.preAggregationsDescription();
      console.log(preAggregationsDescription);

      const queries = tempTablePreAggregations(preAggregationsDescription);

      console.log(JSON.stringify(queries.concat(queryAndParams)));

      return dbRunner.testQueries(
        queries.concat([queryAndParams]).map(q => replaceTableName(q, preAggregationsDescription, 42))
      ).then(res => {
        console.log(JSON.stringify(res));
        res.should.be.deepEqual(
          [
            {
              "visitors__source": "some",
              "visitors__created_at_day": "2017-01-02T00:00:00.000",
              "visitors__count": 1
            },
            {
              "visitors__source": "some",
              "visitors__created_at_day": "2017-01-04T00:00:00.000",
              "visitors__count": 1
            },
            {
              "visitors__source": "google",
              "visitors__created_at_day": "2017-01-05T00:00:00.000",
              "visitors__count": 1
            }
          ]
        );
      });
    });
  });
});
