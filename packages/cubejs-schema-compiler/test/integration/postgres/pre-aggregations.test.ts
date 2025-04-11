import { PreAggregationPartitionRangeLoader } from '@cubejs-backend/query-orchestrator';
import { PostgresQuery } from '../../../src/adapter/PostgresQuery';
import { BigqueryQuery } from '../../../src/adapter/BigqueryQuery';
import { prepareJsCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

describe('PreAggregations', () => {
  jest.setTimeout(200000);

  // language=JavaScript
  const { compiler, joinGraph, cubeEvaluator } = prepareJsCompiler(`
    cube(\`visitors\`, {
      sql: \`
      select * from visitors WHERE \${FILTER_PARAMS.visitors.createdAt.filter('created_at')}
      AND \${FILTER_PARAMS.ReferenceOriginalSql.createdAt.filter('created_at')}
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

        checkinsRollingTotal: {
          sql: \`\${checkinsCount}\`,
          type: 'sum',
          rollingWindow: {
            trailing: 'unbounded'
          }
        },

        checkinsRolling2day: {
          sql: \`\${checkinsCount}\`,
          type: 'sum',
          rollingWindow: {
            trailing: '2 day'
          }
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
        },

        googleUniqueSourceCount: {
          sql: \`\${CUBE.source}\`,
          filters: [{
            sql: \`\${CUBE}.source = 'google'\`
          }],
          type: 'countDistinct'
        },


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
        sourceAndId: {
          type: 'string',
          sql: \`\${source} || '_' || \${id}\`,
        },
        createdAt: {
          type: 'time',
          sql: 'created_at',
          granularities: {
            hourTenMinOffset: {
              interval: '1 hour',
              offset: '10 minutes'
            }
          }
        },
        signedUpAt: {
          type: 'time',
          sql: \`\${createdAt}\`
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
        uniqueSourceCountRollup: {
          type: 'rollup',
          measures: [uniqueSourceCount],
          dimensions: [source],
          timeDimension: createdAt,
          granularity: 'day'
        },
        googleUniqueSourceCountRollup: {
          type: 'rollup',
          measures: [googleUniqueSourceCount],
          dimensions: [source],
          timeDimension: signedUpAt,
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
        partitionedRolling: {
          type: 'rollup',
          measureReferences: [checkinsRollingTotal, checkinsRolling2day],
          dimensionReferences: [source],
          timeDimensionReference: createdAt,
          granularity: 'hour',
          partitionGranularity: 'month'
        },
        countCustomGranularity: {
          measures: [count],
          timeDimension: createdAt,
          granularity: 'hourTenMinOffset',
          allowNonStrictDateRangeMatch: false
        },
        sourceAndIdRollup: {
          measures: [count],
          dimensions: [sourceAndId, source],
          timeDimension: createdAt,
          granularity: 'hour',
          allowNonStrictDateRangeMatch: true
        },
        visitorsMultiplied: {
          measures: [count],
          dimensions: [visitor_checkins.source],
          timeDimension: createdAt,
          granularity: 'day',
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

    cube('ReferenceOriginalSql', {
      extends: visitors,
      sql: \`select v.* from \${visitors.sql()} v where v.source = 'google'\`,

      preAggregations: {
        partitioned: {
          type: 'rollup',
          measureReferences: [count],
          dimensionReferences: [source],
          timeDimensionReference: createdAt,
          granularity: 'day',
          partitionGranularity: 'month',
          refreshKey: {
            every: '1 hour',
            incremental: true,
            updateWindow: '1 day'
          },
          useOriginalSqlPreAggregations: true
        }
      }
    })

    cube('LambdaVisitors', {
      extends: visitors,

      preAggregations: {
        partitionedLambda: {
          type: 'rollupLambda',
          rollups: [partitioned, RealTimeLambdaVisitors.partitioned]
        },
        partitioned: {
          type: 'rollup',
          measures: [count],
          dimensions: [id, source],
          timeDimension: createdAt,
          granularity: 'day',
          partitionGranularity: 'month',
          refreshKey: {
            every: '1 hour',
            incremental: true,
            updateWindow: '1 day'
          }
        }
      }
    });

    cube('RealTimeLambdaVisitors', {
      dataSource: 'ksql',
      extends: visitors,

      preAggregations: {
        partitioned: {
          type: 'rollup',
          measures: [count],
          dimensions: [id, source],
          timeDimension: createdAt,
          granularity: 'day',
          build_range_start: { sql: "SELECT DATE_SUB(NOW(), interval '96 hour')" },
          build_range_end: { sql: "SELECT NOW()" },
          partitionGranularity: 'day'
        }
      }
    });

    view('visitors_view', {
      cubes: [{
        join_path: visitors,
        includes: '*'
      }]
    });
    `);

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

    return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
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

  it('simple pre-aggregation (allowNonStrictDateRangeMatch: true)', () => compiler.compile().then(() => {
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors.count'
      ],
      timeDimensions: [{
        dimension: 'visitors.createdAt',
        dateRange: ['2017-01-01 00:10:00.000', '2017-01-29 22:59:59.999'],
        granularity: 'hour',
      }],
      timezone: 'America/Los_Angeles',
      preAggregationsSchema: ''
    });

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);
    expect(query.preAggregations?.preAggregationForQuery?.canUsePreAggregation).toEqual(true);
    expect(queryAndParams[0]).toMatch(/visitors_source_and_id_rollup/);

    return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
      expect(res).toEqual(
        [
          {
            visitors__count: '1',
            visitors__created_at_hour: '2017-01-02T16:00:00.000Z',
          },
          {
            visitors__count: '1',
            visitors__created_at_hour: '2017-01-04T16:00:00.000Z',
          },
          {
            visitors__count: '1',
            visitors__created_at_hour: '2017-01-05T16:00:00.000Z',
          },
          {
            visitors__count: '2',
            visitors__created_at_hour: '2017-01-06T16:00:00.000Z',
          },
        ]
      );
    });
  }));

  it('simple pre-aggregation with custom granularity (exact match)', () => compiler.compile().then(() => {
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors.count'
      ],
      timeDimensions: [{
        dimension: 'visitors.createdAt',
        dateRange: ['2017-01-01 00:10:00.000', '2017-01-29 22:09:59.999'],
        granularity: 'hourTenMinOffset',
      }],
      timezone: 'America/Los_Angeles',
      preAggregationsSchema: ''
    });

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);
    expect(query.preAggregations?.preAggregationForQuery?.canUsePreAggregation).toEqual(true);
    expect(queryAndParams[0]).toMatch(/visitors_count_custom_granularity/);

    return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
      expect(res).toEqual(
        [
          {
            visitors__count: '1',
            visitors__created_at_hourTenMinOffset: '2017-01-02T15:10:00.000Z',
          },
          {
            visitors__count: '1',
            visitors__created_at_hourTenMinOffset: '2017-01-04T15:10:00.000Z',
          },
          {
            visitors__count: '1',
            visitors__created_at_hourTenMinOffset: '2017-01-05T15:10:00.000Z',
          },
          {
            visitors__count: '2',
            visitors__created_at_hourTenMinOffset: '2017-01-06T15:10:00.000Z',
          },
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

    return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
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

  it('leaf measure view pre-aggregation', () => compiler.compile().then(() => {
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors_view.ratio'
      ],
      timeDimensions: [{
        dimension: 'visitors_view.createdAt',
        granularity: 'day',
        dateRange: ['2017-01-01', '2017-01-30']
      }],
      timezone: 'America/Los_Angeles',
      order: [{
        id: 'visitors_view.createdAt'
      }],
      preAggregationsSchema: ''
    });

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);
    const preAggregationsDescription = query.preAggregations?.preAggregationsDescription();
    console.log(preAggregationsDescription);
    expect((<any>preAggregationsDescription)[0].loadSql[0]).toMatch(/visitors_ratio/);

    return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
      expect(res).toEqual(
        [
          {
            visitors_view__created_at_day: '2017-01-02T00:00:00.000Z',
            visitors_view__ratio: '0.33333333333333333333'
          },
          {
            visitors_view__created_at_day: '2017-01-04T00:00:00.000Z',
            visitors_view__ratio: '0.50000000000000000000'
          },
          {
            visitors_view__created_at_day: '2017-01-05T00:00:00.000Z',
            visitors_view__ratio: '1.00000000000000000000'
          },
          {
            visitors_view__created_at_day: '2017-01-06T00:00:00.000Z',
            visitors_view__ratio: null
          }
        ]
      );
    });
  }));

  it('non-additive measure view pre-aggregation', () => compiler.compile().then(() => {
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors_view.uniqueSourceCount'
      ],
      timeDimensions: [{
        dimension: 'visitors_view.signedUpAt',
        granularity: 'day',
        dateRange: ['2017-01-01', '2017-01-30']
      }],
      timezone: 'America/Los_Angeles',
      order: [{
        id: 'visitors_view.createdAt'
      }],
      preAggregationsSchema: ''
    });

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);
    const preAggregationsDescription = query.preAggregations?.preAggregationsDescription();
    console.log(preAggregationsDescription);
    expect((<any>preAggregationsDescription)[0].loadSql[0]).toMatch(/visitors_ratio/);

    return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
      expect(res).toEqual(
        [
          {
            visitors_view__signed_up_at_day: '2017-01-02T00:00:00.000Z',
            visitors_view__unique_source_count: '1'
          },
          {
            visitors_view__signed_up_at_day: '2017-01-04T00:00:00.000Z',
            visitors_view__unique_source_count: '1'
          },
          {
            visitors_view__signed_up_at_day: '2017-01-05T00:00:00.000Z',
            visitors_view__unique_source_count: '1'
          },
          {
            visitors_view__signed_up_at_day: '2017-01-06T00:00:00.000Z',
            visitors_view__unique_source_count: '0'
          }
        ]
      );
    });
  }));

  it('non-additive single value view filter', () => compiler.compile().then(() => {
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors_view.uniqueSourceCount'
      ],
      timeDimensions: [{
        dimension: 'visitors_view.signedUpAt',
        granularity: 'day',
        dateRange: ['2017-01-01', '2017-01-30']
      }],
      filters: [{
        dimension: 'visitors_view.source',
        operator: 'equals',
        values: ['google']
      }],
      timezone: 'America/Los_Angeles',
      order: [{
        id: 'visitors_view.createdAt'
      }],
      preAggregationsSchema: ''
    });

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);
    const preAggregationsDescription = query.preAggregations?.preAggregationsDescription();
    console.log(preAggregationsDescription);
    expect((<any>preAggregationsDescription)[0].loadSql[0]).toMatch(/visitors_unique_source_count/);

    return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
      expect(res).toEqual(
        [
          {
            visitors_view__signed_up_at_day: '2017-01-05T00:00:00.000Z',
            visitors_view__unique_source_count: '1'
          }
        ]
      );
    });
  }));

  it('non-additive single value view filtered measure', () => compiler.compile().then(() => {
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors_view.googleUniqueSourceCount'
      ],
      timeDimensions: [{
        dimension: 'visitors_view.signedUpAt',
        granularity: 'day',
        dateRange: ['2017-01-01', '2017-01-30']
      }],
      filters: [{
        dimension: 'visitors_view.source',
        operator: 'equals',
        values: ['google']
      }],
      timezone: 'America/Los_Angeles',
      order: [{
        id: 'visitors_view.createdAt'
      }],
      preAggregationsSchema: ''
    });

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);
    const preAggregationsDescription = query.preAggregations?.preAggregationsDescription();
    console.log(preAggregationsDescription);
    expect((<any>preAggregationsDescription)[0].loadSql[0]).toMatch(/visitors_google_unique_source_count/);

    return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
      expect(res).toEqual(
        [
          {
            visitors_view__signed_up_at_day: '2017-01-05T00:00:00.000Z',
            visitors_view__google_unique_source_count: '1'
          }
        ]
      );
    });
  }));

  it('multiplied measure no match', () => compiler.compile().then(() => {
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors.count'
      ],
      dimensions: ['visitor_checkins.source'],
      order: [{
        id: 'visitor_checkins.source'
      }],
      timezone: 'America/Los_Angeles',
      preAggregationsSchema: ''
    });

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);
    expect(queryAndParams[0]).toMatch(/count\(distinct/ig);
    const preAggregationsDescription = query.preAggregations?.preAggregationsDescription();
    console.log(preAggregationsDescription);
    expect((<any>preAggregationsDescription).filter(p => p.type === 'rollup').length).toBe(0);

    return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
      expect(res).toEqual(
        [
          {
            vc__source: 'google',
            visitors__count: '1'
          },
          {
            vc__source: null,
            visitors__count: '6'
          },
        ]
      );
    });
  }));

  it('multiplied measure match', () => compiler.compile().then(() => {
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors.count'
      ],
      dimensions: ['visitor_checkins.source'],
      timeDimensions: [{
        dimension: 'visitors.createdAt',
        granularity: 'day',
        dateRange: ['2017-01-01', '2017-01-30']
      }],
      order: [{
        id: 'visitors.createdAt'
      }, {
        id: 'visitor_checkins.source'
      }],
      timezone: 'America/Los_Angeles',
      preAggregationsSchema: ''
    });

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);
    const preAggregationsDescription = query.preAggregations?.preAggregationsDescription();
    console.log(preAggregationsDescription);
    expect((<any>preAggregationsDescription)[0].loadSql[0]).toMatch(/multiplied/);

    return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
      expect(res).toEqual(
        [
          {
            vc__source: 'google',
            visitors__count: '1',
            visitors__created_at_day: '2017-01-02T00:00:00.000Z',
          },
          {
            vc__source: null,
            visitors__count: '1',
            visitors__created_at_day: '2017-01-02T00:00:00.000Z',
          },
          {
            vc__source: null,
            visitors__count: '1',
            visitors__created_at_day: '2017-01-04T00:00:00.000Z',
          },
          {
            vc__source: null,
            visitors__count: '1',
            visitors__created_at_day: '2017-01-05T00:00:00.000Z',
          },
          {
            vc__source: null,
            visitors__count: '2',
            visitors__created_at_day: '2017-01-06T00:00:00.000Z',
          },
        ]
      );
    });
  }));

  it('non-leaf additive measure', () => compiler.compile().then(() => {
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors_view.count'
      ],
      dimensions: ['visitors_view.sourceAndId'],
      timezone: 'America/Los_Angeles',
      order: [{
        id: 'visitors_view.sourceAndId'
      }],
      preAggregationsSchema: ''
    });

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);
    const preAggregationsDescription = query.preAggregations?.preAggregationsDescription();
    console.log(preAggregationsDescription);
    expect((<any>preAggregationsDescription)[0].loadSql[0]).toMatch(/visitors_source_and_id_rollup/);

    return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
      expect(res).toEqual(
        [
          {
            visitors_view__count: '1',
            visitors_view__source_and_id: 'google_3'
          },
          {
            visitors_view__count: '1',
            visitors_view__source_and_id: 'some_1'
          },
          {
            visitors_view__count: '1',
            visitors_view__source_and_id: 'some_2'
          },
          {
            visitors_view__count: '3',
            visitors_view__source_and_id: null
          }
        ]
      );
    });
  }));

  it('non-leaf additive measure with time dimension', () => compiler.compile().then(() => {
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors_view.count'
      ],
      dimensions: ['visitors_view.sourceAndId'],
      timezone: 'America/Los_Angeles',
      timeDimensions: [{
        dimension: 'visitors_view.signedUpAt',
        granularity: 'day',
        dateRange: ['2017-01-01', '2017-01-30']
      }],
      order: [{
        id: 'visitors_view.createdAt',
      }, {
        id: 'visitors_view.sourceAndId'
      }],
      preAggregationsSchema: ''
    });

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);
    const preAggregationsDescription = query.preAggregations?.preAggregationsDescription();
    console.log(preAggregationsDescription);
    expect((<any>preAggregationsDescription)[0].loadSql[0]).toMatch(/visitors_source_and_id_rollup/);

    return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
      expect(res).toEqual(
        [
          {
            visitors_view__count: '1',
            visitors_view__signed_up_at_day: '2017-01-05T00:00:00.000Z',
            visitors_view__source_and_id: 'google_3'
          },
          {
            visitors_view__count: '1',
            visitors_view__signed_up_at_day: '2017-01-02T00:00:00.000Z',
            visitors_view__source_and_id: 'some_1'
          },
          {
            visitors_view__count: '1',
            visitors_view__signed_up_at_day: '2017-01-04T00:00:00.000Z',
            visitors_view__source_and_id: 'some_2'
          },
          {
            visitors_view__count: '2',
            visitors_view__signed_up_at_day: '2017-01-06T00:00:00.000Z',
            visitors_view__source_and_id: null
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

    return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
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

    return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
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
    expect(preAggregationsDescription[0].invalidateKeyQueries[0][1][0]).toEqual(
      PreAggregationPartitionRangeLoader.TO_PARTITION_RANGE
    );

    return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
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

  it('reference original sql', () => compiler.compile().then(() => {
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'ReferenceOriginalSql.count'
      ],
      dimensions: [
        'ReferenceOriginalSql.source'
      ],
      timeDimensions: [{
        dimension: 'ReferenceOriginalSql.createdAt',
        granularity: 'day',
        dateRange: ['2017-01-01', '2017-01-25']
      }],
      timezone: 'America/Los_Angeles',
      order: [{
        id: 'ReferenceOriginalSql.createdAt'
      }],
      preAggregationsSchema: ''
    });

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);
    const preAggregationsDescription: any = query.preAggregations?.preAggregationsDescription();
    console.log(JSON.stringify(preAggregationsDescription, null, 2));

    // For extended cubes pre-aggregations from parents are treated as local
    expect(preAggregationsDescription[0].tableName).toEqual('reference_original_sql_default');

    return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
      expect(res).toEqual(
        [
          {
            reference_original_sql__source: 'google',
            reference_original_sql__created_at_day: '2017-01-05T00:00:00.000Z',
            reference_original_sql__count: '1'
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

    return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
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

    const queries = dbRunner.tempTablePreAggregations(preAggregationsDescription);
    expect(preAggregationsDescription[1].loadSql[0]).toMatch(/vc_main/);

    console.log(JSON.stringify(queries.concat(queryAndParams)));

    return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
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

    const queries = dbRunner.tempTablePreAggregations(preAggregationsDescription);

    const desc = preAggregationsDescription.find(e => e.tableName === 'visitors_multi_stage');
    expect(desc.invalidateKeyQueries[0][1][0]).toEqual(PreAggregationPartitionRangeLoader.TO_PARTITION_RANGE);

    const vcMainDesc = preAggregationsDescription.find(e => e.tableName === 'vc_main');
    expect(vcMainDesc.invalidateKeyQueries.length).toEqual(1);

    console.log(JSON.stringify(queries.concat(queryAndParams)));

    return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
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
          '2017-01-06',
          '2017-01-31'
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

    expect(partitionedTables[0].invalidateKeyQueries[0][2].updateWindowSeconds).toEqual(86400 * 7);
    expect(partitionedTables[0].invalidateKeyQueries[0][2].renewalThresholdOutsideUpdateWindow).toEqual(86400);

    const queries = dbRunner.tempTablePreAggregations(preAggregationsDescription);

    console.log(JSON.stringify(queries.concat(queryAndParams)));

    return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
      console.log(JSON.stringify(res));
      expect(res).toEqual(
        [{
          visitors__checkins_total: '0',
          visitors__created_at_day: '2017-01-06T00:00:00.000Z',
          visitors__source: null,
        }]
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

    const queries = dbRunner.tempTablePreAggregations(preAggregationsDescription);

    console.log(JSON.stringify(queries.concat(queryAndParams)));

    return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
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

    const queries = dbRunner.tempTablePreAggregations(preAggregationsDescription);

    console.log(JSON.stringify(queries.concat(queryAndParams)));

    return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
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

    const queries = dbRunner.tempTablePreAggregations(preAggregationsDescription);

    expect(queries.filter(([q]) => !!q.match(/3600/)).length).toBeGreaterThanOrEqual(1);

    console.log(JSON.stringify(queries.concat(queryAndParams)));

    return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
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

  it('partitioned rolling', () => compiler.compile().then(() => {
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors.checkinsRollingTotal',
      ],
      dimensions: [
        'visitors.source'
      ],
      timezone: 'UTC',
      preAggregationsSchema: '',
      timeDimensions: [{
        dimension: 'visitors.createdAt',
        granularity: 'day',
        dateRange: ['2017-01-02', '2017-01-05']
      }],
      order: [{
        id: 'visitors.createdAt'
      }, {
        id: 'visitors.source'
      }],
    });

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);
    const preAggregationsDescription = query.preAggregations?.preAggregationsDescription();
    console.log(preAggregationsDescription);

    const queries = dbRunner.tempTablePreAggregations(preAggregationsDescription);

    console.log(JSON.stringify(queries.concat(queryAndParams)));

    return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
      console.log(JSON.stringify(res));
      expect(res).toEqual(
        [
          {
            visitors__source: null,
            visitors__created_at_day: '2017-01-02T00:00:00.000Z',
            visitors__checkins_rolling_total: '0'
          },
          {
            visitors__source: 'some',
            visitors__created_at_day: '2017-01-03T00:00:00.000Z',
            visitors__checkins_rolling_total: '3'
          },
          {
            visitors__source: null,
            visitors__created_at_day: '2017-01-03T00:00:00.000Z',
            visitors__checkins_rolling_total: '0'
          },
          {
            visitors__source: 'some',
            visitors__created_at_day: '2017-01-04T00:00:00.000Z',
            visitors__checkins_rolling_total: '3'
          },
          {
            visitors__source: null,
            visitors__created_at_day: '2017-01-04T00:00:00.000Z',
            visitors__checkins_rolling_total: '0'
          },
          {
            visitors__source: 'some',
            visitors__created_at_day: '2017-01-05T00:00:00.000Z',
            visitors__checkins_rolling_total: '5'
          },
          {
            visitors__source: null,
            visitors__created_at_day: '2017-01-05T00:00:00.000Z',
            visitors__checkins_rolling_total: '0'
          },
        ]
      );
    });
  }));

  it('partitioned rolling 2 day', () => compiler.compile().then(() => {
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors.checkinsRolling2day',
      ],
      dimensions: [
        'visitors.source'
      ],
      timezone: 'UTC',
      preAggregationsSchema: '',
      timeDimensions: [{
        dimension: 'visitors.createdAt',
        granularity: 'day',
        dateRange: ['2017-01-02', '2017-01-05']
      }],
      order: [{
        id: 'visitors.createdAt'
      }, {
        id: 'visitors.source'
      }],
    });

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);
    const preAggregationsDescription = query.preAggregations?.preAggregationsDescription();
    console.log(preAggregationsDescription);

    const queries = dbRunner.tempTablePreAggregations(preAggregationsDescription);

    console.log(JSON.stringify(queries.concat(queryAndParams)));

    return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
      console.log(JSON.stringify(res));
      expect(res).toEqual(
        [
          {
            visitors__source: null,
            visitors__created_at_day: '2017-01-02T00:00:00.000Z',
            visitors__checkins_rolling2day: null
          },
          {
            visitors__source: 'some',
            visitors__created_at_day: '2017-01-03T00:00:00.000Z',
            visitors__checkins_rolling2day: '3'
          },
          {
            visitors__source: 'some',
            visitors__created_at_day: '2017-01-04T00:00:00.000Z',
            visitors__checkins_rolling2day: '3'
          },
          {
            visitors__source: 'some',
            visitors__created_at_day: '2017-01-05T00:00:00.000Z',
            visitors__checkins_rolling2day: '2'
          },
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

    const queries = dbRunner.tempTablePreAggregations(preAggregationsDescription);

    console.log(JSON.stringify(queries.concat(queryAndParams)));

    return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
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

    const queries = dbRunner.tempTablePreAggregations(preAggregationsDescription);

    expect(queries.filter(([q]) => !!q.match(/3600/)).length).toBeGreaterThanOrEqual(1);

    console.log(JSON.stringify(queries.concat(queryAndParams)));

    return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
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

    const queries = dbRunner.tempTablePreAggregations(preAggregationsDescription);

    console.log(JSON.stringify(queries.concat(queryAndParams)));

    return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
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

  it('rollup join existing joins', async () => {
    await compiler.compile();

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

    const queries = dbRunner.tempTablePreAggregations(preAggregationsDescription);

    console.log(JSON.stringify(queries.concat(queryAndParams)));

    return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
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
  });

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

    const queries = dbRunner.tempTablePreAggregations(preAggregationsDescription);

    console.log(JSON.stringify(queries.concat(queryAndParams)));

    return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
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
    });

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);
    const preAggregationsDescription = query.preAggregations?.preAggregationsDescription();
    console.log(preAggregationsDescription);

    const queries = dbRunner.tempTablePreAggregations(preAggregationsDescription);

    console.log(JSON.stringify(queries.concat(queryAndParams)));

    return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
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
    let query;

    for (let i = 0; i < 10; i++) {
      query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
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

    const queries = dbRunner.tempTablePreAggregations(preAggregationsDescription);

    console.log(JSON.stringify(queries.concat(queryAndParams)));

    return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
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

  it('simple view', () => compiler.compile().then(() => {
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors_view.checkinsTotal'
      ],
      dimensions: [
        'visitors_view.source'
      ],
      timezone: 'America/Los_Angeles',
      preAggregationsSchema: '',
      timeDimensions: [{
        dimension: 'visitors_view.createdAt',
        granularity: 'day',
        dateRange: ['2016-12-30', '2017-01-05']
      }],
      order: [{
        id: 'visitors_view.createdAt'
      }],
    });

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);
    const preAggregationsDescription = query.preAggregations?.preAggregationsDescription();
    console.log(JSON.stringify(preAggregationsDescription, null, 2));

    expect((<any>preAggregationsDescription)[0].loadSql[0]).toMatch(/visitors_partitioned/);

    const queries = dbRunner.tempTablePreAggregations(preAggregationsDescription);

    console.log(JSON.stringify(queries.concat(queryAndParams)));

    return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
      console.log(JSON.stringify(res));
      expect(res).toEqual(
        [
          {
            visitors_view__source: 'some',
            visitors_view__created_at_day: '2017-01-02T00:00:00.000Z',
            visitors_view__checkins_total: '3'
          },
          {
            visitors_view__source: 'some',
            visitors_view__created_at_day: '2017-01-04T00:00:00.000Z',
            visitors_view__checkins_total: '2'
          },
          {
            visitors_view__source: 'google',
            visitors_view__created_at_day: '2017-01-05T00:00:00.000Z',
            visitors_view__checkins_total: '1'
          }
        ]
      );
    });
  }));

  it('simple view non matching time-dimension granularity', () => compiler.compile().then(() => {
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors_view.checkinsTotal'
      ],
      dimensions: [
        'visitors_view.source'
      ],
      timezone: 'America/Los_Angeles',
      preAggregationsSchema: '',
      timeDimensions: [{
        dimension: 'visitors_view.createdAt',
        granularity: 'month',
        dateRange: ['2016-12-30', '2017-01-05']
      }],
      order: [{
        id: 'visitors_view.createdAt'
      }],
    });

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);
    const preAggregationsDescription = query.preAggregations?.preAggregationsDescription();
    console.log(JSON.stringify(preAggregationsDescription, null, 2));

    expect((<any>preAggregationsDescription)[0].loadSql[0]).toMatch(/visitors_partitioned/);

    const queries = dbRunner.tempTablePreAggregations(preAggregationsDescription);

    console.log(JSON.stringify(queries.concat(queryAndParams)));

    return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
      console.log(JSON.stringify(res));
      expect(res).toEqual(
        [
          {
            visitors_view__source: 'google',
            visitors_view__created_at_month: '2017-01-01T00:00:00.000Z',
            visitors_view__checkins_total: '1'
          },
          {
            visitors_view__source: 'some',
            visitors_view__created_at_month: '2017-01-01T00:00:00.000Z',
            visitors_view__checkins_total: '5'
          }
        ]
      );
    });
  }));

  it('lambda cross data source refresh key and ungrouped', () => compiler.compile().then(() => {
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'LambdaVisitors.count'
      ],
      dimensions: [
        'LambdaVisitors.source'
      ],
      timeDimensions: [{
        dimension: 'LambdaVisitors.createdAt',
        granularity: 'day',
        dateRange: ['2017-01-01', '2017-01-25']
      }],
      timezone: 'America/Los_Angeles',
      order: [{
        id: 'LambdaVisitors.createdAt'
      }],
      preAggregationsSchema: '',
      queryFactory: {
        createQuery: (cube, compilers, options) => {
          if (cube === 'RealTimeLambdaVisitors') {
            // eslint-disable-next-line global-require
            const { KsqlQuery } = require('../../../../../cubejs-ksql-driver');
            return new KsqlQuery(compilers, options);
          } else {
            return new PostgresQuery(compilers, options);
          }
        }
      }
    });

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);
    const preAggregationsDescription: any = query.preAggregations?.preAggregationsDescription();
    console.log(JSON.stringify(preAggregationsDescription, null, 2));
    const { partitionInvalidateKeyQueries, loadSql } = preAggregationsDescription.find(p => p.preAggregationId === 'RealTimeLambdaVisitors.partitioned');

    expect(partitionInvalidateKeyQueries).toStrictEqual([]);
    expect(loadSql[0]).not.toMatch(/GROUP BY/);
    expect(loadSql[0]).toMatch(/THEN 1 END `real_time_lambda_visitors__count`/);
  }));
});
