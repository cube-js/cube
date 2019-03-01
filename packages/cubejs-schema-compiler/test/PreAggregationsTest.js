const R = require('ramda');

const PostgresQuery = require('../adapter/PostgresQuery');
const BigqueryQuery = require('../adapter/BigqueryQuery');
const PrepareCompiler = require('./PrepareCompiler');
require('should');

const prepareCompiler = PrepareCompiler.prepareCompiler;
const dbRunner = require('./DbRunner');

describe('PreAggregations', () => {
  const { compiler, joinGraph, cubeEvaluator } = prepareCompiler(`
    cube(\`visitors\`, {
      sql: \`
      select * from visitors
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
          sql: \`\${uniqueSourceCount} / nullif(\${checkinsTotal}, 0)\`,
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
          type: 'originalSql',
          refreshKey: {
            sql: 'select NOW()'
          }
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
          partitionGranularity: 'day',
          refreshKey: {
            sql: \`SELECT CASE WHEN \${FILTER_PARAMS.visitors.createdAt.filter((from, to) => \`\${to}::timestamp > now()\`)} THEN now() END\`
          }
        }
      }
    })
    
    
    cube('visitor_checkins', {
      sql: \`
      select * from visitor_checkins
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
      preAggregation.reduce((replacedQuery, desc) =>
        replacedQuery.replace(new RegExp(desc.tableName, 'g'), desc.tableName + '_' + suffix), toReplace
      ),
      params
    ];
  }

  function tempTablePreAggregations(preAggregationsDescriptions) {
    return R.unnest(preAggregationsDescriptions.map(desc =>
      desc.invalidateKeyQueries.concat([
        [desc.loadSql[0].replace('CREATE TABLE', 'CREATE TEMP TABLE'), desc.loadSql[1]]
      ])
    ));
  }

  it('simple pre-aggregation', () => {
    return compiler.compile().then(() => {
      const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
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
              "visitors.created_at_date": "2017-01-02T00:00:00.000Z",
              "visitors.count": "1"
            },
            {
              "visitors.created_at_date": "2017-01-04T00:00:00.000Z",
              "visitors.count": "1"
            },
            {
              "visitors.created_at_date": "2017-01-05T00:00:00.000Z",
              "visitors.count": "1"
            },
            {
              "visitors.created_at_date": "2017-01-06T00:00:00.000Z",
              "visitors.count": "2"
            }
          ]
        );
      });
    });
  });

  it('leaf measure pre-aggregation', () => {
    return compiler.compile().then(() => {
      const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: [
          'visitors.ratio'
        ],
        timeDimensions: [{
          dimension: 'visitors.createdAt',
          granularity: 'date',
          dateRange: ['2017-01-01', '2017-01-30']
        }],
        timezone: 'America/Los_Angeles',
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
              "visitors.created_at_date": "2017-01-02T00:00:00.000Z",
              "visitors.ratio": '0.33333333333333333333'
            },
            {
              "visitors.created_at_date": "2017-01-04T00:00:00.000Z",
              "visitors.ratio": '0.50000000000000000000'
            },
            {
              "visitors.created_at_date": "2017-01-05T00:00:00.000Z",
              "visitors.ratio": '1.00000000000000000000'
            },
            {
              "visitors.created_at_date": "2017-01-06T00:00:00.000Z",
              "visitors.ratio": null
            }
          ]
        );
      });
    });
  });

  it('inherited original sql', () => {
    return compiler.compile().then(() => {
      const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: [
          'GoogleVisitors.count'
        ],
        timeDimensions: [{
          dimension: 'GoogleVisitors.createdAt',
          granularity: 'date',
          dateRange: ['2017-01-01', '2017-01-30']
        }],
        timezone: 'America/Los_Angeles',
        order: [{
          id: 'GoogleVisitors.createdAt'
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
              "google_visitors.created_at_date": "2017-01-05T00:00:00.000Z",
              "google_visitors.count": "1"
            }
          ]
        );
      });
    });
  });

  it('hll bigquery rollup', () => {
    return compiler.compile().then(() => {
      const query = new BigqueryQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: [
          'visitors.countDistinctApprox'
        ],
        timeDimensions: [{
          dimension: 'visitors.createdAt',
          granularity: 'date',
          dateRange: ['2017-01-01', '2017-01-30']
        }],
        timezone: 'America/Los_Angeles',
        order: [{
          id: 'visitors.createdAt'
        }],
        preAggregationsSchema: ''
      });

      const queryAndParams = query.buildSqlAndParams();
      console.log(queryAndParams);
      const preAggregationsDescription = query.preAggregations.preAggregationsDescription()[0];
      console.log(preAggregationsDescription);

      queryAndParams[0].should.match(/HLL_COUNT\.MERGE/);
      preAggregationsDescription.loadSql[0].should.match(/HLL_COUNT\.INIT/);
    });
  });

  it('sub query', () => {
    return compiler.compile().then(() => {
      const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: [
          'visitors.count'
        ],
        order: [{ id: 'visitors.checkinsCount' }],
        dimensions: ['visitors.checkinsCount'],
        timezone: 'America/Los_Angeles',
        preAggregationsSchema: ''
      });

      const queryAndParams = query.buildSqlAndParams();
      console.log(queryAndParams);
      const preAggregationsDescription = query.preAggregations.preAggregationsDescription();
      console.log(preAggregationsDescription);

      const queries = tempTablePreAggregations(preAggregationsDescription);

      console.log(JSON.stringify(queries.concat(queryAndParams)));

      return dbRunner.testQueries(
        queries.concat([queryAndParams]).map(q => replaceTableName(q, preAggregationsDescription, 2))
      ).then(res => {
        console.log(JSON.stringify(res));
        res.should.be.deepEqual(
          [
            { "visitors.checkins_count": "0", "visitors.count": "3" },
            { "visitors.checkins_count": "1", "visitors.count": "1" },
            { "visitors.checkins_count": "2", "visitors.count": "1" },
            { "visitors.checkins_count": "3", "visitors.count": "1" }
          ]
        );
      });
    });
  });

  it('multi-stage', () => {
    return compiler.compile().then(() => {
      const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: [
          'visitors.checkinsTotal'
        ],
        timezone: 'America/Los_Angeles',
        preAggregationsSchema: '',
        timeDimensions: [{
          dimension: 'visitors.createdAt',
          granularity: 'month',
          dateRange: ['2017-01-01', '2017-01-30']
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

      const desc = preAggregationsDescription.find(desc => desc.tableName === 'visitors_multi_stage20170101');
      desc.invalidateKeyQueries[0][1][0].should.be.equal("2017-01-02T07:59:59Z");

      console.log(JSON.stringify(queries.concat(queryAndParams)));

      return dbRunner.testQueries(
        queries.concat([queryAndParams]).map(q => replaceTableName(q, preAggregationsDescription, 3))
      ).then(res => {
        console.log(JSON.stringify(res));
        res.should.be.deepEqual(
          [
            {
              'visitors.created_at_month': '2017-01-01T00:00:00.000Z',
              'visitors.checkins_total': '6'
            }
          ]
        );
      });
    });
  });

  it('partitioned', () => {
    return compiler.compile().then(() => {
      const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: [
          'visitors.checkinsTotal'
        ],
        dimensions: [
          'visitors.source'
        ],
        timezone: 'America/Los_Angeles',
        preAggregationsSchema: '',
        timeDimensions: [{
          dimension: 'visitors.createdAt',
          granularity: 'date',
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
              "visitors.source": "some",
              "visitors.created_at_date": "2017-01-02T00:00:00.000Z",
              "visitors.checkins_total": "3"
            },
            {
              "visitors.source": "some",
              "visitors.created_at_date": "2017-01-04T00:00:00.000Z",
              "visitors.checkins_total": "2"
            },
            {
              "visitors.source": "google",
              "visitors.created_at_date": "2017-01-05T00:00:00.000Z",
              "visitors.checkins_total": "1"
            }
          ]
        );
      });
    });
  });

  it('segment', () => {
    return compiler.compile().then(() => {
      const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: [
          'visitors.checkinsTotal'
        ],
        dimensions: [],
        segments: ['visitors.google'],
        timezone: 'America/Los_Angeles',
        preAggregationsSchema: '',
        timeDimensions: [{
          dimension: 'visitors.createdAt',
          granularity: 'date',
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
        queries.concat([queryAndParams]).map(q => replaceTableName(q, preAggregationsDescription, 142))
      ).then(res => {
        console.log(JSON.stringify(res));
        res.should.be.deepEqual(
          [
            {
              "visitors.created_at_date": "2017-01-05T00:00:00.000Z",
              "visitors.checkins_total": "1"
            }
          ]
        );
      });
    });
  });

  it('partitioned without time', () => {
    return compiler.compile().then(() => {
      const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: [
          'visitors.checkinsTotal'
        ],
        dimensions: [
          'visitors.source'
        ],
        timezone: 'America/Los_Angeles',
        preAggregationsSchema: '',
        timeDimensions: [],
        order: [],
      });

      const queryAndParams = query.buildSqlAndParams();
      console.log(queryAndParams);
      const preAggregationsDescription = query.preAggregations.preAggregationsDescription();
      console.log(preAggregationsDescription);

      const queries = tempTablePreAggregations(preAggregationsDescription);

      console.log(JSON.stringify(queries.concat(queryAndParams)));

      return dbRunner.testQueries(
        queries.concat([queryAndParams]).map(q => replaceTableName(q, preAggregationsDescription, 43))
      ).then(res => {
        console.log(JSON.stringify(res));
        res.should.be.deepEqual(
          [
            { "visitors.source": "some", "visitors.checkins_total": "5" },
            { "visitors.source": "google", "visitors.checkins_total": "1" },
            { "visitors.source": null, "visitors.checkins_total": "0" }
          ]
        );
      });
    });
  });
});
