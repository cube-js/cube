import R from 'ramda';
import { PostgresQuery } from '../../../src/adapter/PostgresQuery';
import { BigqueryQuery } from '../../../src/adapter/BigqueryQuery';
import { prepareCompiler } from '../../unit/PrepareCompiler';
import { PostgresDBRunner } from './PostgresDBRunner';

const dbRunner = new PostgresDBRunner();

describe('PreAggregations', () => {
  jest.setTimeout(200000);

  afterAll(async () => {
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
          sql: \`\${CUBE.id} = \${visitor_checkins.visitor_id}\`
        },
        
        cards: {
          relationship: 'hasMany',
          sql: \`\${visitors.id} = \${cards.visitorId}\`
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
          type: 'rollup',
          measureReferences: [checkinsTotal],
          segmentReferences: [google],
          timeDimensionReference: createdAt,
          granularity: 'week',
        },
        approx: {
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
          partitionGranularity: 'month',
          refreshKey: {
            every: '1 hour',
            incremental: true,
            updateWindow: '7 day'
          }
        },
        partitionedHourly: {
          type: 'rollup',
          measureReferences: [checkinsTotal],
          dimensionReferences: [source],
          timeDimensionReference: createdAt,
          granularity: 'hour',
          partitionGranularity: 'hour'
        },
        partitionedHourlyForRange: {
          type: 'rollup',
          measureReferences: [checkinsTotal],
          dimensionReferences: [source, createdAt],
          timeDimensionReference: createdAt,
          granularity: 'hour',
          partitionGranularity: 'hour'
        },
        ratioRollup: {
          type: 'rollup',
          measureReferences: [checkinsTotal, uniqueSourceCount],
          timeDimensionReference: createdAt,
          granularity: 'day'
        },
        forJoin: {
          type: 'rollup',
          dimensionReferences: [id, source]
        },
        forJoinIncCards: {
          type: 'rollup',
          dimensionReferences: [id, source, cards.visitorId]
        },
        partitionedHourlyForJoin: {
          type: 'rollup',
          dimensionReferences: [id, source],
          timeDimensionReference: createdAt,
          granularity: 'hour',
          partitionGranularity: 'hour'
        },
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
        forJoin: {
          type: 'rollup',
          measureReferences: [count],
          dimensionReferences: [visitor_id]
        },
        joined: {
          type: 'rollupJoin',
          measureReferences: [count],
          dimensionReferences: [visitors.source],
          rollupReferences: [visitor_checkins.forJoin, visitors.forJoin],
        },
        joinedPartitioned: {
          type: 'rollupJoin',
          measureReferences: [count],
          dimensionReferences: [visitors.source],
          timeDimensionReference: visitors.createdAt,
          granularity: 'hour',
          rollupReferences: [visitor_checkins.forJoin, visitors.partitionedHourlyForJoin],
        },
        joinedIncCards: {
          type: 'rollupJoin',
          measureReferences: [count],
          dimensionReferences: [visitors.source, cards.visitorId],
          rollupReferences: [visitor_checkins.forJoin, visitors.forJoinIncCards],
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
    });
    
    cube('cards', {
      sql: \`
      select * from cards
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
        
        visitorId: {
          type: 'number',
          sql: 'visitor_id'
        }
      },
      
      preAggregations: {
        forJoin: {
          type: 'rollup',
          measureReferences: [count],
          dimensionReferences: [visitorId]
        },
      }
    });
    
    cube('GoogleVisitors', {
      refreshKey: {
        immutable: true,
      },
      extends: visitors,
      sql: \`select v.* from \${visitors.sql()} v where v.source = 'google'\`
    })
    
    cube('EveryHourVisitors', {
      refreshKey: {
        immutable: true,
      },
      extends: visitors,
      sql: \`select v.* from \${visitors.sql()} v where v.source = 'google'\`,
      
      preAggregations: {
        default: {
          type: 'originalSql',
          refreshKey: {
            sql: 'select NOW()'
          }
        },
        partitioned: {
          type: 'rollup',
          measureReferences: [checkinsTotal],
          dimensionReferences: [source],
          timeDimensionReference: createdAt,
          granularity: 'day',
          partitionGranularity: 'month',
          refreshKey: {
            every: '1 hour',
            incremental: true,
            updateWindow: '1 day'
          }
        }
      }
    })
    
    cube('EmptyHourVisitors', {
      extends: EveryHourVisitors,
      sql: \`select v.* from \${visitors.sql()} v where created_at < '2000-01-01'\`
    })
    `);

  function replaceTableName(query, preAggregation, suffix) {
    const [toReplace, params] = query;
    console.log(toReplace);
    preAggregation = Array.isArray(preAggregation) ? preAggregation : [preAggregation];
    return [
      preAggregation.reduce(
        (replacedQuery, desc) => replacedQuery
          .replace(new RegExp(desc.tableName, 'g'), `${desc.tableName}_${suffix}`)
          .replace(/CREATE INDEX (?!i_)/, `CREATE INDEX i_${suffix}_`),
        toReplace
      ),
      params
    ];
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

  it('simple pre-aggregation', () => compiler.compile().then(() => {
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
    console.log(queryAndParams);
    const preAggregationsDescription = query.preAggregations?.preAggregationsDescription();
    console.log(preAggregationsDescription);

    return dbRunner.testQueries(tempTablePreAggregations(preAggregationsDescription).concat([
      query.buildSqlAndParams()
    ]).map(q => replaceTableName(q, preAggregationsDescription, 1))).then(res => {
      expect(res).toEqual(
        [
          {
            visitors__created_at_day: '2017-01-02T00:00:00.000Z',
            visitors__count: '1'
          },
          {
            visitors__created_at_day: '2017-01-04T00:00:00.000Z',
            visitors__count: '1'
          },
          {
            visitors__created_at_day: '2017-01-05T00:00:00.000Z',
            visitors__count: '1'
          },
          {
            visitors__created_at_day: '2017-01-06T00:00:00.000Z',
            visitors__count: '2'
          }
        ]
      );
    });
  }));

  it('leaf measure pre-aggregation', () => compiler.compile().then(() => {
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors.ratio'
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
    console.log(queryAndParams);
    const preAggregationsDescription = query.preAggregations?.preAggregationsDescription();
    console.log(preAggregationsDescription);
    expect((<any>preAggregationsDescription)[0].loadSql[0]).toMatch(/visitors_ratio/);

    return dbRunner.testQueries(tempTablePreAggregations(preAggregationsDescription).concat([
      query.buildSqlAndParams()
    ]).map(q => replaceTableName(q, preAggregationsDescription, 10))).then(res => {
      expect(res).toEqual(
        [
          {
            visitors__created_at_day: '2017-01-02T00:00:00.000Z',
            visitors__ratio: '0.33333333333333333333'
          },
          {
            visitors__created_at_day: '2017-01-04T00:00:00.000Z',
            visitors__ratio: '0.50000000000000000000'
          },
          {
            visitors__created_at_day: '2017-01-05T00:00:00.000Z',
            visitors__ratio: '1.00000000000000000000'
          },
          {
            visitors__created_at_day: '2017-01-06T00:00:00.000Z',
            visitors__ratio: null
          }
        ]
      );
    });
  }));

  it('inherited original sql', () => compiler.compile().then(() => {
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'GoogleVisitors.count'
      ],
      timeDimensions: [{
        dimension: 'GoogleVisitors.createdAt',
        granularity: 'day',
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
    const preAggregationsDescription = query.preAggregations?.preAggregationsDescription();
    console.log(preAggregationsDescription);

    return dbRunner.testQueries(tempTablePreAggregations(preAggregationsDescription).concat([
      query.buildSqlAndParams()
    ]).map(q => replaceTableName(q, preAggregationsDescription, 1))).then(res => {
      expect(res).toEqual(
        [
          {
            google_visitors__created_at_day: '2017-01-05T00:00:00.000Z',
            google_visitors__count: '1'
          }
        ]
      );
    });
  }));

  it('immutable partition default refreshKey', () => compiler.compile().then(() => {
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'GoogleVisitors.checkinsTotal'
      ],
      dimensions: [
        'GoogleVisitors.source'
      ],
      timeDimensions: [{
        dimension: 'GoogleVisitors.createdAt',
        granularity: 'day',
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
    const preAggregationsDescription: any = query.preAggregations?.preAggregationsDescription();
    console.log(JSON.stringify(preAggregationsDescription, null, 2));

    expect(preAggregationsDescription[0].invalidateKeyQueries[0][0]).toMatch(/NOW\(\) </);

    return dbRunner.testQueries(tempTablePreAggregations(preAggregationsDescription).concat([
      query.buildSqlAndParams()
    ]).map(q => replaceTableName(q, preAggregationsDescription, 101))).then(res => {
      expect(res).toEqual(
        [
          {
            google_visitors__source: 'google',
            google_visitors__created_at_day: '2017-01-05T00:00:00.000Z',
            google_visitors__checkins_total: '1'
          }
        ]
      );
    });
  }));

  it('immutable every hour', () => compiler.compile().then(() => {
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'EveryHourVisitors.checkinsTotal'
      ],
      dimensions: [
        'EveryHourVisitors.source'
      ],
      timeDimensions: [{
        dimension: 'EveryHourVisitors.createdAt',
        granularity: 'day',
        dateRange: ['2017-01-01', '2017-01-25']
      }],
      timezone: 'America/Los_Angeles',
      order: [{
        id: 'EveryHourVisitors.createdAt'
      }],
      preAggregationsSchema: ''
    });

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);
    const preAggregationsDescription: any = query.preAggregations?.preAggregationsDescription();
    console.log(JSON.stringify(preAggregationsDescription, null, 2));

    expect(preAggregationsDescription[0].invalidateKeyQueries[0][0]).toMatch(/NOW\(\) </);
    expect(preAggregationsDescription[0].invalidateKeyQueries[0][1][0]).toEqual('2017-02-01T07:59:59Z');

    return dbRunner.testQueries(tempTablePreAggregations(preAggregationsDescription).concat([
      query.buildSqlAndParams()
    ]).map(q => replaceTableName(q, preAggregationsDescription, 103))).then(res => {
      expect(res).toEqual(
        [
          {
            every_hour_visitors__source: 'google',
            every_hour_visitors__created_at_day: '2017-01-05T00:00:00.000Z',
            every_hour_visitors__checkins_total: '1'
          }
        ]
      );
    });
  }));

  it('partitioned scheduled refresh', () => compiler.compile().then(async () => {
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitor_checkins.count'
      ],
      timeDimensions: [{
        dimension: 'EveryHourVisitors.createdAt',
        granularity: 'day',
        dateRange: ['2017-01-01', '2017-01-25']
      }],
      timezone: 'UTC',
      order: [{
        id: 'EveryHourVisitors.createdAt'
      }],
      preAggregationsSchema: ''
    });

    const preAggregations = cubeEvaluator.scheduledPreAggregations();
    const partitionedPreAgg =
        preAggregations.find(p => p.preAggregationName === 'partitioned' && p.cube === 'visitor_checkins');

    const minMaxQueries = query.preAggregationStartEndQueries('visitor_checkins', partitionedPreAgg?.preAggregation);

    console.log(minMaxQueries);

    expect(minMaxQueries[0][0]).toMatch(/NOW/);

    const res = await dbRunner.testQueries(minMaxQueries);

    expect(res).toEqual(
      [{ max: '2017-01-06T00:00:00.000Z' }]
    );
  }));

  it('empty scheduled refresh', () => compiler.compile().then(async () => {
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitor_checkins.count'
      ],
      timeDimensions: [{
        dimension: 'EmptyHourVisitors.createdAt',
        granularity: 'hour',
        dateRange: ['2017-01-01', '2017-01-25']
      }],
      timezone: 'UTC',
      order: [{
        id: 'EmptyHourVisitors.createdAt'
      }],
      preAggregationsSchema: ''
    });

    const preAggregations = cubeEvaluator.scheduledPreAggregations();
    const partitionedPreAgg =
        preAggregations.find(p => p.preAggregationName === 'emptyPartitioned' && p.cube === 'visitor_checkins');

    const minMaxQueries = query.preAggregationStartEndQueries('visitor_checkins', partitionedPreAgg?.preAggregation);

    console.log(minMaxQueries);

    const res = await dbRunner.testQueries(minMaxQueries);

    expect(res).toEqual(
      [{ max: null }]
    );
  }));

  it('mutable partition default refreshKey', () => compiler.compile().then(() => {
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors.checkinsTotal'
      ],
      dimensions: [
        'visitors.source'
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
    console.log(queryAndParams);
    const preAggregationsDescription: any = query.preAggregations?.preAggregationsDescription();
    console.log(JSON.stringify(preAggregationsDescription, null, 2));

    expect(preAggregationsDescription[0].invalidateKeyQueries[0][0]).toMatch(/FLOOR/);

    return dbRunner.testQueries(tempTablePreAggregations(preAggregationsDescription).concat([
      query.buildSqlAndParams()
    ]).map(q => replaceTableName(q, preAggregationsDescription, 102))).then(res => {
      expect(res).toEqual(
        [
          {
            visitors__source: 'some',
            visitors__created_at_day: '2017-01-02T00:00:00.000Z',
            visitors__checkins_total: '3'
          },
          {
            visitors__source: 'some',
            visitors__created_at_day: '2017-01-04T00:00:00.000Z',
            visitors__checkins_total: '2'
          },
          {
            visitors__source: 'google',
            visitors__created_at_day: '2017-01-05T00:00:00.000Z',
            visitors__checkins_total: '1'
          },
          {
            visitors__source: null,
            visitors__created_at_day: '2017-01-06T00:00:00.000Z',
            visitors__checkins_total: '0'
          }
        ]
      );
    });
  }));

  it('hll bigquery rollup', () => compiler.compile().then(() => {
    const query = new BigqueryQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors.countDistinctApprox'
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
    console.log(queryAndParams);
    const preAggregationsDescription: any = query.preAggregations?.preAggregationsDescription()[0];
    console.log(preAggregationsDescription);

    expect(queryAndParams[0]).toMatch(/HLL_COUNT\.MERGE/);
    expect(preAggregationsDescription.loadSql[0]).toMatch(/HLL_COUNT\.INIT/);
  }));

  it('sub query', () => compiler.compile().then(() => {
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
    const preAggregationsDescription: any = query.preAggregations?.preAggregationsDescription();
    console.log(preAggregationsDescription);

    const queries = tempTablePreAggregations(preAggregationsDescription);
    expect(preAggregationsDescription[1].loadSql[0]).toMatch(/vc_main/);

    console.log(JSON.stringify(queries.concat(queryAndParams)));

    return dbRunner.testQueries(
      queries.concat([queryAndParams]).map(q => replaceTableName(q, preAggregationsDescription, 2))
    ).then(res => {
      console.log(JSON.stringify(res));
      expect(res).toEqual(
        [
          { visitors__checkins_count: '0', visitors__count: '3' },
          { visitors__checkins_count: '1', visitors__count: '1' },
          { visitors__checkins_count: '2', visitors__count: '1' },
          { visitors__checkins_count: '3', visitors__count: '1' }
        ]
      );
    });
  }));

  it('multi-stage', () => compiler.compile().then(() => {
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors.checkinsTotal'
      ],
      timezone: 'America/Los_Angeles',
      preAggregationsSchema: '',
      timeDimensions: [{
        dimension: 'visitors.createdAt',
        granularity: 'month',
        dateRange: ['2017-01-01', '2017-01-31']
      }],
      order: [{
        id: 'visitors.createdAt'
      }],
    });

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);
    const preAggregationsDescription: any = query.preAggregations?.preAggregationsDescription();
    console.log(preAggregationsDescription);

    const queries = tempTablePreAggregations(preAggregationsDescription);

    const desc = preAggregationsDescription.find(e => e.tableName === 'visitors_multi_stage20170101');
    expect(desc.invalidateKeyQueries[0][1][0]).toEqual('2017-01-02T07:59:59Z');

    const vcMainDesc = preAggregationsDescription.find(e => e.tableName === 'vc_main');
    expect(vcMainDesc.invalidateKeyQueries.length).toEqual(1);

    console.log(JSON.stringify(queries.concat(queryAndParams)));

    return dbRunner.testQueries(
      queries.concat([queryAndParams]).map(q => replaceTableName(q, preAggregationsDescription, 3))
    ).then(res => {
      console.log(JSON.stringify(res));
      expect(res).toEqual(
        [
          {
            visitors__created_at_month: '2017-01-01T00:00:00.000Z',
            visitors__checkins_total: '6'
          }
        ]
      );
    });
  }));

  it('incremental renewal threshold', () => compiler.compile().then(() => {
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
        granularity: 'day',
        dateRange: [
          new Date(new Date().getTime() - 60 * 24 * 60 * 60 * 1000).toJSON().substring(0, 10),
          new Date().toJSON().substring(0, 10)
        ]
      }],
      order: [{
        id: 'visitors.createdAt'
      }],
    });

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);
    const preAggregationsDescription: any = query.preAggregations?.preAggregationsDescription();
    console.log(JSON.stringify(preAggregationsDescription, null, 2));
    const partitionedTables = preAggregationsDescription
      .filter(({ tableName }) => tableName.indexOf('visitors_partitioned') === 0);

    expect(partitionedTables[0].refreshKeyRenewalThresholds[0]).toEqual(86400);
    expect(partitionedTables[partitionedTables.length - 1].refreshKeyRenewalThresholds[0]).toEqual(300);

    const queries = tempTablePreAggregations(preAggregationsDescription);

    console.log(JSON.stringify(queries.concat(queryAndParams)));

    return dbRunner.testQueries(
      queries.concat([queryAndParams]).map(q => replaceTableName(q, preAggregationsDescription, 1042))
    ).then(res => {
      console.log(JSON.stringify(res));
      expect(res).toEqual(
        []
      );
    });
  }));

  it('partitioned', () => compiler.compile().then(() => {
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
        granularity: 'day',
        dateRange: ['2016-12-30', '2017-01-05']
      }],
      order: [{
        id: 'visitors.createdAt'
      }],
    });

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);
    const preAggregationsDescription = query.preAggregations?.preAggregationsDescription();
    console.log(JSON.stringify(preAggregationsDescription, null, 2));

    const queries = tempTablePreAggregations(preAggregationsDescription);

    console.log(JSON.stringify(queries.concat(queryAndParams)));

    return dbRunner.testQueries(
      queries.concat([queryAndParams]).map(q => replaceTableName(q, preAggregationsDescription, 42))
    ).then(res => {
      console.log(JSON.stringify(res));
      expect(res).toEqual(
        [
          {
            visitors__source: 'some',
            visitors__created_at_day: '2017-01-02T00:00:00.000Z',
            visitors__checkins_total: '3'
          },
          {
            visitors__source: 'some',
            visitors__created_at_day: '2017-01-04T00:00:00.000Z',
            visitors__checkins_total: '2'
          },
          {
            visitors__source: 'google',
            visitors__created_at_day: '2017-01-05T00:00:00.000Z',
            visitors__checkins_total: '1'
          }
        ]
      );
    });
  }));

  it('partitioned inDateRange', () => compiler.compile().then(() => {
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors.checkinsTotal'
      ],
      dimensions: [
        'visitors.source'
      ],
      timezone: 'America/Los_Angeles',
      preAggregationsSchema: '',
      filters: [{
        member: 'visitors.createdAt',
        operator: 'inDateRange',
        values: ['2016-12-30', '2017-01-05']
      }],
      order: [{
        id: 'visitors.checkinsTotal'
      }],
    });

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);
    const preAggregationsDescription = query.preAggregations?.preAggregationsDescription();
    console.log(JSON.stringify(preAggregationsDescription, null, 2));

    const queries = tempTablePreAggregations(preAggregationsDescription);

    console.log(JSON.stringify(queries.concat(queryAndParams)));

    return dbRunner.testQueries(
      queries.concat([queryAndParams]).map(q => replaceTableName(q, preAggregationsDescription, 12342))
    ).then(res => {
      console.log(JSON.stringify(res));
      expect(res).toEqual(
        [
          {
            visitors__source: 'google',
            visitors__checkins_total: '1'
          },
          {
            visitors__source: 'some',
            visitors__checkins_total: '5'
          }
        ]
      );
    });
  }));

  it('partitioned hourly', () => compiler.compile().then(() => {
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors.checkinsTotal'
      ],
      dimensions: [
        'visitors.source'
      ],
      timezone: 'UTC',
      preAggregationsSchema: '',
      timeDimensions: [{
        dimension: 'visitors.createdAt',
        granularity: 'hour',
        dateRange: ['2017-01-02', '2017-01-05']
      }],
      order: [{
        id: 'visitors.createdAt'
      }],
    });

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);
    const preAggregationsDescription = query.preAggregations?.preAggregationsDescription();
    console.log(preAggregationsDescription);

    const queries = tempTablePreAggregations(preAggregationsDescription);

    console.log(JSON.stringify(queries.concat(queryAndParams)));

    return dbRunner.testQueries(
      queries.concat([queryAndParams]).map(q => replaceTableName(q, preAggregationsDescription, 242))
    ).then(res => {
      console.log(JSON.stringify(res));
      expect(res).toEqual(
        [
          {
            visitors__source: 'some',
            visitors__created_at_hour: '2017-01-03T00:00:00.000Z',
            visitors__checkins_total: '3'
          },
          {
            visitors__source: 'some',
            visitors__created_at_hour: '2017-01-05T00:00:00.000Z',
            visitors__checkins_total: '2'
          }
        ]
      );
    });
  }));

  it('not aligned time dimension', () => compiler.compile().then(() => {
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors.checkinsTotal'
      ],
      dimensions: [
        'visitors.source'
      ],
      timezone: 'UTC',
      preAggregationsSchema: '',
      timeDimensions: [{
        dimension: 'visitors.createdAt',
        granularity: 'hour',
        dateRange: ['2017-01-02T00:00:00.000', '2017-01-05T00:15:59.999']
      }],
      order: [{
        id: 'visitors.createdAt'
      }],
    });

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);
    const preAggregationsDescription: any = query.preAggregations?.preAggregationsDescription();
    console.log(preAggregationsDescription);
    expect(preAggregationsDescription.length).toEqual(2);

    const queries = tempTablePreAggregations(preAggregationsDescription);

    console.log(JSON.stringify(queries.concat(queryAndParams)));

    return dbRunner.testQueries(
      queries.concat([queryAndParams]).map(q => replaceTableName(q, preAggregationsDescription, 342))
    ).then(res => {
      console.log(JSON.stringify(res));
      expect(res).toEqual(
        [
          {
            visitors__source: 'some',
            visitors__created_at_hour: '2017-01-03T00:00:00.000Z',
            visitors__checkins_total: '3'
          },
          {
            visitors__source: 'some',
            visitors__created_at_hour: '2017-01-05T00:00:00.000Z',
            visitors__checkins_total: '2'
          }
        ]
      );
    });
  }));

  it('segment', () => compiler.compile().then(() => {
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
        granularity: 'week',
        dateRange: ['2016-12-26', '2017-01-08']
      }],
      order: [{
        id: 'visitors.createdAt'
      }],
    });

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);
    const preAggregationsDescription = query.preAggregations?.preAggregationsDescription();
    console.log(preAggregationsDescription);

    const queries = tempTablePreAggregations(preAggregationsDescription);

    console.log(JSON.stringify(queries.concat(queryAndParams)));

    return dbRunner.testQueries(
      queries.concat([queryAndParams]).map(q => replaceTableName(q, preAggregationsDescription, 142))
    ).then(res => {
      console.log(JSON.stringify(res));
      expect(res).toEqual(
        [
          {
            visitors__created_at_week: '2017-01-02T00:00:00.000Z',
            visitors__checkins_total: '1'
          }
        ]
      );
    });
  }));

  it('rollup join', () => compiler.compile().then(() => {
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitor_checkins.count',
      ],
      dimensions: ['visitors.source'],
      preAggregationsSchema: '',
      order: [{
        id: 'visitors.source',
      }],
    });

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);
    const preAggregationsDescription: any = query.preAggregations?.preAggregationsDescription();
    console.log(preAggregationsDescription);

    console.log(query.preAggregations?.rollupMatchResultDescriptions());

    const queries = tempTablePreAggregations(preAggregationsDescription);

    console.log(JSON.stringify(queries.concat(queryAndParams)));

    return dbRunner.testQueries(
      queries.concat([queryAndParams]).map(q => replaceTableName(q, preAggregationsDescription, 342)),
    ).then(res => {
      console.log(JSON.stringify(res));
      expect(res).toEqual(
        [
          { visitors__source: 'google', vc__count: '1' },
          { visitors__source: 'some', vc__count: '5' },
          { visitors__source: null, vc__count: null },
        ],
      );
    });
  }));

  it('rollup join existing joins', () => compiler.compile().then(() => {
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitor_checkins.count',
      ],
      dimensions: ['visitors.source', 'cards.visitorId'],
      preAggregationsSchema: '',
      order: [{
        id: 'visitors.source',
      }, {
        id: 'cards.visitorId',
      }],
    });

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);
    const preAggregationsDescription = query.preAggregations?.preAggregationsDescription();
    console.log(preAggregationsDescription);

    console.log(query.preAggregations?.rollupMatchResultDescriptions());

    const queries = tempTablePreAggregations(preAggregationsDescription);

    console.log(JSON.stringify(queries.concat(queryAndParams)));

    return dbRunner.testQueries(
      queries.concat([queryAndParams]).map(q => replaceTableName(q, preAggregationsDescription, 341)),
    ).then(res => {
      console.log(JSON.stringify(res));
      expect(res).toEqual(
        [
          { visitors__source: 'google', cards__visitor_id: 3, vc__count: '1' },
          { visitors__source: 'some', cards__visitor_id: 1, vc__count: '3' },
          { visitors__source: 'some', cards__visitor_id: null, vc__count: '2' },
          { visitors__source: null, cards__visitor_id: null, vc__count: null },
        ],
      );
    });
  }));

  it('rollup join partitioned', () => compiler.compile().then(() => {
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitor_checkins.count',
      ],
      dimensions: ['visitors.source'],
      timezone: 'America/Los_Angeles',
      preAggregationsSchema: '',
      timeDimensions: [{
        dimension: 'visitors.createdAt',
        granularity: 'hour',
        dateRange: ['2017-01-03', '2017-01-04']
      }],
      order: [{
        id: 'visitors.createdAt'
      }],
    });

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);
    const preAggregationsDescription = query.preAggregations?.preAggregationsDescription();
    console.log(preAggregationsDescription);

    console.log(query.preAggregations?.rollupMatchResultDescriptions());

    const queries = tempTablePreAggregations(preAggregationsDescription);

    console.log(JSON.stringify(queries.concat(queryAndParams)));

    return dbRunner.testQueries(
      queries.concat([queryAndParams]).map(q => replaceTableName(q, preAggregationsDescription, 442)),
    ).then(res => {
      console.log(JSON.stringify(res));
      expect(res).toEqual(
        [
          {
            visitors__source: 'some',
            visitors__created_at_hour: '2017-01-04T16:00:00.000Z',
            vc__count: '2'
          }
        ],
      );
    });
  }));

  it('partitioned without time', () => compiler.compile().then(() => {
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
    const preAggregationsDescription = query.preAggregations?.preAggregationsDescription();
    console.log(preAggregationsDescription);

    const queries = tempTablePreAggregations(preAggregationsDescription);

    console.log(JSON.stringify(queries.concat(queryAndParams)));

    return dbRunner.testQueries(
      queries.concat([queryAndParams]).map(q => replaceTableName(q, preAggregationsDescription, 43))
    ).then(res => {
      console.log(JSON.stringify(res));
      expect(res).toEqual(
        [
          { visitors__source: 'some', visitors__checkins_total: '5' },
          { visitors__source: 'google', visitors__checkins_total: '1' },
          { visitors__source: null, visitors__checkins_total: '0' }
        ]
      );
    });
  }));

  it('partitioned huge span', () => compiler.compile().then(() => {
    let queryAndParams;
    let preAggregationsDescription;

    for (let i = 0; i < 10; i++) {
      const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: [
          'visitors.checkinsTotal'
        ],
        dimensions: [
          'visitors.source'
        ],
        timezone: 'UTC',
        preAggregationsSchema: '',
        timeDimensions: [{
          dimension: 'visitors.createdAt',
          granularity: 'day',
          dateRange: ['2000-12-30', '2017-01-06']
        }],
        order: [{
          id: 'visitors.createdAt'
        }],
      });
      queryAndParams = query.buildSqlAndParams();
      preAggregationsDescription = query.preAggregations?.preAggregationsDescription();
    }

    console.log(queryAndParams);
    console.log(preAggregationsDescription);

    const queries = tempTablePreAggregations(preAggregationsDescription);

    console.log(JSON.stringify(queries.concat(queryAndParams)));

    return dbRunner.testQueries(
      queries.concat([queryAndParams]).map(q => replaceTableName(q, preAggregationsDescription, 1142))
    ).then(res => {
      console.log(JSON.stringify(res));
      expect(res).toEqual(
        [
          {
            visitors__source: null,
            visitors__created_at_day: '2016-09-07T00:00:00.000Z',
            visitors__checkins_total: '0'
          },
          {
            visitors__source: 'some',
            visitors__created_at_day: '2017-01-03T00:00:00.000Z',
            visitors__checkins_total: '3'
          },
          {
            visitors__source: 'some',
            visitors__created_at_day: '2017-01-05T00:00:00.000Z',
            visitors__checkins_total: '2'
          },
          {
            visitors__source: 'google',
            visitors__created_at_day: '2017-01-06T00:00:00.000Z',
            visitors__checkins_total: '1'
          }
        ]
      );
    });
  }));
});

describe('PreAggregations in time hierarchy', () => {
  // this.timeout(200000);

  afterAll(async () => {
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
        }
      },

      dimensions: {
        createdAt: {
          type: 'time',
          sql: 'created_at'
        },
      },
      
      preAggregations: {
        month: {
          type: 'rollup',
          measureReferences: [count],
          timeDimensionReference: createdAt,
          granularity: 'month',
        },
        day: {
          type: 'rollup',
          measureReferences: [count],
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
      preAggregation.reduce((replacedQuery, desc) => replacedQuery.replace(new RegExp(desc.tableName, 'g'), `${desc.tableName}_${suffix}`), toReplace),
      params
    ];
  }

  function tempTablePreAggregations(preAggregationsDescriptions) {
    return R.unnest(preAggregationsDescriptions.map(desc => desc.invalidateKeyQueries.concat([
      [desc.loadSql[0].replace('CREATE TABLE', 'CREATE TEMP TABLE'), desc.loadSql[1]]
    ])));
  }

  it('query on year match to pre-agg on month', () => compiler.compile().then(() => {
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors.count'
      ],
      dimensions: [],
      timezone: 'America/Los_Angeles',
      timeDimensions: [{
        dimension: 'visitors.createdAt',
        granularity: 'year',
        dateRange: ['2016-12-01', '2018-12-31']
      }],
      preAggregationsSchema: '',
      order: [],
    });

    const queryAndParams = query.buildSqlAndParams();

    expect((<any>query).preAggregations.preAggregationForQuery.preAggregation.granularity).toEqual('month');

    console.log(queryAndParams);
    const preAggregationsDescription = query.preAggregations?.preAggregationsDescription();
    console.log(preAggregationsDescription);

    const queries = tempTablePreAggregations(preAggregationsDescription);

    console.log(JSON.stringify(queries.concat(queryAndParams)));

    return dbRunner.testQueries(
      queries.concat([queryAndParams]).map(q => replaceTableName(q, preAggregationsDescription, 1))
    ).then(res => {
      console.log(JSON.stringify(res));
      expect(res).toEqual(
        [
          {
            visitors__count: '5',
            visitors__created_at_year: '2017-01-01T00:00:00.000Z'
          },
        ]
      );
    });
  }));

  it('query on week match to pre-agg on day', () => compiler.compile().then(() => {
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors.count'
      ],
      dimensions: [],
      timezone: 'America/Los_Angeles',
      timeDimensions: [{
        dimension: 'visitors.createdAt',
        granularity: 'week',
        dateRange: ['2017-01-02', '2019-02-08']
      }],
      preAggregationsSchema: '',
      order: [],
      filters: [
        {
          or: [{
            dimension: 'visitors.count',
            operator: 'equals',
            values: [
              '5'
            ]
          }, {
            dimension: 'visitors.count',
            operator: 'equals',
            values: [
              '2'
            ]
          },
          ]
        }]
    });

    const queryAndParams = query.buildSqlAndParams();

    expect((<any>query).preAggregations.preAggregationForQuery.preAggregation.granularity).toEqual('day');

    console.log(queryAndParams);
    const preAggregationsDescription = query.preAggregations?.preAggregationsDescription();
    console.log(preAggregationsDescription);

    const queries = tempTablePreAggregations(preAggregationsDescription);

    console.log(JSON.stringify(queries.concat(queryAndParams)));

    return dbRunner.testQueries(
      queries.concat([queryAndParams]).map(q => replaceTableName(q, preAggregationsDescription, 1))
    ).then(res => {
      console.log(JSON.stringify(res));
      expect(res).toEqual(
        [
          {
            visitors__count: '5',
            visitors__created_at_week: '2017-01-02T00:00:00.000Z'
          },
        ]
      );
    });
  }));
});
