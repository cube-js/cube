/* globals describe,it,after */
/* eslint-disable quote-props */
const R = require('ramda');
require('should');

const MSSqlQuery = require('../adapter/MssqlQuery');
const { prepareCompiler } = require('./PrepareCompiler');

const dbRunner = require('./MSSqlDbRunner');

describe('MSSqlPreAggregations', function test() {
  this.timeout(20000);

  after(async () => {
    await dbRunner.tearDown();
  });

  const { compiler, joinGraph, cubeEvaluator } = prepareCompiler(`
    cube(\`visitors\`, {
      sql: \`
      select * from ##visitors
      \`,
      
      joins: {
        visitor_checkins: {
          relationship: 'hasMany',
          sql: \`\${CUBE}.id = \${visitor_checkins}.visitor_id\`
        }
      },

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
        },
        checkinsCount: {
          type: 'number',
          sql: \`\${visitor_checkins.count}\`,
          subQuery: true
        }
      },
      
      segments: {
        google: {
          sql: \`source = 'google'\`
        }
      },
      
      preAggregations: {
        default: {
          type: 'originalSql'
        },
        googleRollup: {
          type: 'rollup',
          measureReferences: [checkinsTotal],
          segmentReferences: [google],
          timeDimensionReference: createdAt,
          granularity: 'day',
        },
        approx: {
          type: 'rollup',
          measureReferences: [countDistinctApprox],
          timeDimensionReference: createdAt,
          granularity: 'day'
        },
        ratio: {
          type: 'rollup',
          measureReferences: [checkinsTotal, uniqueSourceCount],
          timeDimensionReference: createdAt,
          granularity: 'day'
        },
        partitioned: {
          type: 'rollup',
          measureReferences: [checkinsTotal],
          dimensionReferences: [source],
          timeDimensionReference: createdAt,
          granularity: 'day',
          partitionGranularity: 'month'
        },
        multiStage: {
          useOriginalSqlPreAggregations: true,
          type: 'rollup',
          measureReferences: [checkinsTotal],
          timeDimensionReference: createdAt,
          granularity: 'month',
          partitionGranularity: 'day'
        }
      }
    })
    
    
    cube('visitor_checkins', {
      sql: \`
      select * from ##visitor_checkins
      \`,

      measures: {
        count: {
          type: 'count'
        }
      },

      dimensions: {
        id: {
          type: 'number',
          sql: 'id',
          primaryKey: true
        },
        visitor_id: {
          type: 'number',
          sql: 'visitor_id'
        },
        source: {
          type: 'string',
          sql: 'source'
        },
        created_at: {
          type: 'time',
          sql: 'created_at'
        }
      },
      
      preAggregations: {
        main: {
          type: 'originalSql'
        },
        auto: {
          type: 'autoRollup',
          maxPreAggregations: 20
        }
      }
    })
    
    cube('GoogleVisitors', {
      extends: visitors,
      sql: \`select v.* from \${visitors.sql()} v where v.source = 'google'\`
    })
    `);

  function replaceTableName(query, preAggregation, suffix) {
    const [toReplace, params] = query;
    console.log(toReplace);
    preAggregation = Array.isArray(preAggregation) ? preAggregation : [preAggregation];
    return [
      preAggregation.reduce(
        (replacedQuery, desc) => replacedQuery.replace(new RegExp(desc.tableName, 'g'), `##${desc.tableName}_${suffix}`), toReplace
      ),
      params
    ];
  }

  function tempTablePreAggregations(preAggregationsDescriptions) {
    return R.unnest(preAggregationsDescriptions.map(
      desc => desc.invalidateKeyQueries.concat([[desc.loadSql[0], desc.loadSql[1]]])
    ));
  }

  it('simple pre-aggregation', () => compiler.compile().then(() => {
    const query = new MSSqlQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors.count'
      ],
      timeDimensions: [{
        dimension: 'visitors.createdAt',
        granularity: 'day',
        dateRange: ['2017-01-01', '2017-01-30']
      }],
      timezone: 'UTC',
      order: [{
        id: 'visitors.createdAt'
      }],
      preAggregationsSchema: ''
    });

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);
    const preAggregationsDescription = query.preAggregations.preAggregationsDescription();
    console.log(preAggregationsDescription);

    return dbRunner.testQueries(tempTablePreAggregations(preAggregationsDescription).concat([
      query.buildSqlAndParams()
    ]).map(q => replaceTableName(q, preAggregationsDescription, 1))).then(res => {
      res.should.be.deepEqual(
        [
          {
            "visitors__created_at_day": "2017-01-03T00:00:00.000",
            "visitors__count": 1
          },
          {
            "visitors__created_at_day": "2017-01-05T00:00:00.000",
            "visitors__count": 1
          },
          {
            "visitors__created_at_day": "2017-01-06T00:00:00.000",
            "visitors__count": 1
          },
          {
            "visitors__created_at_day": "2017-01-07T00:00:00.000",
            "visitors__count": 2
          }
        ]
      );
    });
  }));

  it('leaf measure pre-aggregation', () => compiler.compile().then(() => {
    const query = new MSSqlQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors.ratio'
      ],
      timeDimensions: [{
        dimension: 'visitors.createdAt',
        granularity: 'day',
        dateRange: ['2017-01-01', '2017-01-30']
      }],
      timezone: 'UTC',
      order: [{
        id: 'visitors.createdAt'
      }],
      preAggregationsSchema: ''
    });

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);
    const preAggregationsDescription = query.preAggregations.preAggregationsDescription();
    console.log(preAggregationsDescription);
    preAggregationsDescription[0].loadSql[0].should.match(/visitors_ratio/);

    return dbRunner.testQueries(tempTablePreAggregations(preAggregationsDescription).concat([
      query.buildSqlAndParams()
    ]).map(q => replaceTableName(q, preAggregationsDescription, 10))).then(res => {
      res.should.be.deepEqual(
        [
          {
            "visitors__created_at_day": "2017-01-03T00:00:00.000",
            "visitors__ratio": 0.333333333333
          },
          {
            "visitors__created_at_day": "2017-01-05T00:00:00.000",
            "visitors__ratio": 0.5
          },
          {
            "visitors__created_at_day": "2017-01-06T00:00:00.000",
            "visitors__ratio": 1
          },
          {
            "visitors__created_at_day": "2017-01-07T00:00:00.000",
            "visitors__ratio": null
          }
        ]
      );
    });
  }));

  it('segment', () => compiler.compile().then(() => {
    const query = new MSSqlQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors.checkinsTotal'
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
            "visitors__checkins_total": 1
          }
        ]
      );
    });
  }));
});
