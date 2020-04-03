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
          partitionGranularity: 'month',
          scheduledRefresh: true
        },
        googleRollup: {
          type: 'rollup',
          measureReferences: [count],
          segmentReferences: [google],
          timeDimensionReference: createdAt,
          granularity: 'day',
        },
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
          dateRange: ['2016-12-30', '2017-01-30'] // TODO fix MySQL pre-aggregation return incorrect results on DST switch
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
            },
            {
              "visitors__source": null,
              "visitors__created_at_day": "2017-01-06T00:00:00.000",
              "visitors__count": 2
            }
          ]
        );
      });
    });
  });

  it('partitioned scheduled refresh', () => {
    return compiler.compile().then(async () => {
      const query = new MySqlQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: [
          'visitors.count'
        ],
        dimensions: [
          'visitors.source'
        ],
        timezone: 'UTC',
        preAggregationsSchema: '',
        timeDimensions: [{
          dimension: 'visitors.createdAt',
          granularity: 'day',
          dateRange: ['2016-12-30', '2017-01-30']
        }],
        order: [{
          id: 'visitors.createdAt'
        }],
      });

      const preAggregations = cubeEvaluator.scheduledPreAggregations();
      const partitionedPreAgg =
        preAggregations.find(p => p.preAggregationName === 'partitioned' && p.cube === 'visitors');

      const minMaxQueries = query.preAggregationStartEndQueries('visitors', partitionedPreAgg.preAggregation);

      console.log(minMaxQueries);

      const res = await dbRunner.testQueries(minMaxQueries);

      console.log(res);

      res[0][Object.keys(res[0])[0]].should.be.deepEqual('2017-01-07 00:00:00');
    });
  });

  it('segment', () => compiler.compile().then(() => {
    const query = new MySqlQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors.count'
      ],
      dimensions: [],
      segments: ['visitors.google'],
      timezone: 'UTC',
      preAggregationsSchema: '',
      timeDimensions: [{
        dimension: 'visitors.createdAt',
        granularity: 'day',
        dateRange: ['2016-12-30', '2017-01-06']
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
      queries.concat([queryAndParams]).map(q => replaceTableName(q, preAggregationsDescription, 142))
    ).then(res => {
      console.log(JSON.stringify(res));
      res.should.be.deepEqual(
        [
          {
            "visitors__created_at_day": "2017-01-06T00:00:00.000",
            "visitors__count": 1
          }
        ]
      );
    });
  }));
});
