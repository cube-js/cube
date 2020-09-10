/* eslint-disable quote-props */
/* globals it, describe, after */
const R = require('ramda');

const PostgresQuery = require('../adapter/PostgresQuery');
const BigqueryQuery = require('../adapter/BigqueryQuery');
const PrepareCompiler = require('./PrepareCompiler');
const should = require('should');

const { prepareCompiler } = PrepareCompiler;
const dbRunner = require('./DbRunner');

describe('PreAggregations', function test() {
  this.timeout(200000);

  after(async () => {
    await dbRunner.tearDown();
  });

  const { compiler, joinGraph, cubeEvaluator } = prepareCompiler(`
    cube(\`visitors\`, {
      sql: \`
      select * from visitors WHERE \${FILTER_PARAMS.visitors.createdAt.filter('created_at')}
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
          subQuery: true,
          propagateFiltersToSubQuery: true
        }
      },
      
      segments: {
        google: {
          sql: \`source = 'google'\`
        }
      },
      
      preAggregations: { 
        default: { 
          sqlAlias: 'visitors_alias_d',
          type: 'originalSql',
          refreshKey: {
            sql: 'select NOW()'
          },
          indexes: {
            source: {
              columns: ['source', 'created_at']
            }
          },
          partitionGranularity: 'month',
          timeDimensionReference: createdAt
        },
        googleRollup: {
          sqlAlias: 'visitorsAliasGR',
          type: 'rollup',
          measureReferences: [checkinsTotal],
          segmentReferences: [google],
          timeDimensionReference: createdAt,
          granularity: 'week',
        },
        approx: {
          sqlAlias: 'visitorsAliasApp',
          type: 'rollup',
          measureReferences: [countDistinctApprox],
          timeDimensionReference: createdAt,
          granularity: 'day'
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
        },
        partitioned: {
          type: 'rollup',
          measureReferences: [checkinsTotal],
          dimensionReferences: [source],
          timeDimensionReference: createdAt,
          granularity: 'day',
          partitionGranularity: 'month'
        },
        partitionedHourly: {
          type: 'rollup',
          measureReferences: [checkinsTotal],
          dimensionReferences: [source],
          timeDimensionReference: createdAt,
          granularity: 'hour',
          partitionGranularity: 'hour'
        },
        ratio: {
          type: 'rollup',
          measureReferences: [checkinsTotal, uniqueSourceCount],
          timeDimensionReference: createdAt,
          granularity: 'day'
        }
      }
    })
    
    
    cube('visitor_checkins', {
      sql: \`
      select * from visitor_checkins
      \`,
      
      sqlAlias: 'vc',

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
        },
        partitioned: {
          type: 'rollup',
          measureReferences: [count],
          timeDimensionReference: EveryHourVisitors.createdAt,
          granularity: 'day',
          partitionGranularity: 'month',
          scheduledRefresh: true,
          refreshRangeStart: {
            sql: "SELECT NOW() - interval '30 day'"
          },
          refreshKey: {
            every: '1 hour',
            incremental: true,
            updateWindow: '1 day'
          }
        },
        emptyPartitioned: {
          type: 'rollup',
          measureReferences: [count],
          timeDimensionReference: EmptyHourVisitors.createdAt,
          granularity: 'hour',
          partitionGranularity: 'month',
          scheduledRefresh: true,
          refreshKey: {
            every: '1 hour',
            incremental: true,
            updateWindow: '1 day'
          }
        }
      }
    })
     
    `);

  function replaceTableName(query, preAggregation, suffix) {
    const [toReplace, params] = query;
    // console.log("toReplace", toReplace);
    preAggregation = Array.isArray(preAggregation) ? preAggregation : [preAggregation];
    r = [
      preAggregation.reduce(
        (replacedQuery, desc) => replacedQuery
          .replace(new RegExp(desc.tableName, 'g'), desc.tableName + '_' + suffix)
          .replace(/CREATE INDEX (?!i_)/, `CREATE INDEX i_${suffix}_`),
        toReplace
      ),
      params
    ];

    // console.log("result toReplace", r);
    return r
  }

  function tempTablePreAggregations(preAggregationsDescriptions) {
    return R.unnest(preAggregationsDescriptions.map(
      desc => desc.invalidateKeyQueries.concat([
        [desc.loadSql[0].replace('CREATE TABLE', 'CREATE TEMP TABLE'), desc.loadSql[1]]
      ]).concat(
        (desc.indexesSql || []).map(({ sql }) => sql)
      )
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
          granularity: 'day',
          dateRange: ['2017-01-01', '2017-01-30']
        }],
        timezone: 'America/Los_Angeles',
        order: [{
          id: 'visitors.createdAt'
        }],
        preAggregationsSchema: ''
      });

      const queryAndParams = query.buildSqlAndParams();
      console.log("queryAndParams", queryAndParams);
      const preAggregationsDescription = query.preAggregations.preAggregationsDescription();
      console.log("preAggregationsDescription", preAggregationsDescription);
      
      should(preAggregationsDescription[0].tableName).be.equal('visitors_alias_d20170101');
  
      // return dbRunner.testQueries(tempTablePreAggregations(preAggregationsDescription).concat([
      //   query.buildSqlAndParams()
      // ]).map(q => replaceTableName(q, preAggregationsDescription, 1))).then(res => {
      //   res.should.be.deepEqual(
      //     [
      //       {
      //         "visitors__created_at_day": "2017-01-02T00:00:00.000Z",
      //         "visitors__count": "1"
      //       },
      //       {
      //         "visitors__created_at_day": "2017-01-04T00:00:00.000Z",
      //         "visitors__count": "1"
      //       },
      //       {
      //         "visitors__created_at_day": "2017-01-05T00:00:00.000Z",
      //         "visitors__count": "1"
      //       },
      //       {
      //         "visitors__created_at_day": "2017-01-06T00:00:00.000Z",
      //         "visitors__count": "2"
      //       }
      //     ]
      //   );
      // });
    });
  });
 
});
