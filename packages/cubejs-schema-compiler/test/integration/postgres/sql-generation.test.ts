import { getEnv } from '@cubejs-backend/shared';
import { UserError } from '../../../src/compiler/UserError';
import { PostgresQuery } from '../../../src/adapter/PostgresQuery';
import { BigqueryQuery } from '../../../src/adapter/BigqueryQuery';
import { PrestodbQuery } from '../../../src/adapter/PrestodbQuery';
import { prepareJsCompiler, prepareYamlCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';
import { createJoinedCubesSchema } from '../../unit/utils';
import { testWithPreAggregation } from './pre-aggregation-utils';

describe('SQL Generation', () => {
  jest.setTimeout(200000);

  // language=JavaScript
  const { compiler, joinGraph, cubeEvaluator } = prepareJsCompiler(`
    const perVisitorRevenueMeasure = {
      type: 'number',
      sql: new Function('visitor_revenue', 'visitor_count', 'return visitor_revenue + "/" + visitor_count')
    }

    cube(\`visitors_create_dates\`, {
      sql: \`
      select id AS create_date_id, created_at from visitors WHERE \${SECURITY_CONTEXT.source.filter('source')} AND
      \${SECURITY_CONTEXT.sourceArray.filter(sourceArray => \`source in (\${sourceArray.join(',')})\`)}
      \`,

      rewriteQueries: true,

      dimensions: {
        create_date_id: {
          type: 'number',
          sql: 'id',
          primaryKey: true
        },
        create_date_created_at: {
          type: 'time',
          sql: 'created_at',
          granularities: {
            three_days: {
              interval: '3 days',
              title: '3 days',
              origin: '2017-01-01'
            }
          }
        }
      }
    })

    cube(\`visitors\`, {
      sql: \`
      select * from visitors WHERE \${SECURITY_CONTEXT.source.filter('source')} AND
      \${SECURITY_CONTEXT.sourceArray.filter(sourceArray => \`source in (\${sourceArray.join(',')})\`)}
      \`,

      rewriteQueries: true,

      refreshKey: {
        sql: 'SELECT 1',
      },

      joins: {
        visitor_checkins: {
          relationship: 'hasMany',
          sql: \`\${CUBE}.id = \${visitor_checkins}.visitor_id\`
        },
        visitors_create_dates: {
          relationship: 'one_to_one',
          sql: \`\${CUBE}.id = \${visitors_create_dates}.create_date_id\`
        }
      },

      segments: {
        some_source: {
          sql: \`\${CUBE}.source = 'some'\`
        }
      },

      measures: {
        visitor_count: {
          type: 'number',
          sql: \`count(*)\`,
          aliases: ['users count']
        },
        revenue: {
          type: 'sum',
          sql: 'amount',
        },
        revenue_1d_d1_dd: {
          type: 'sum',
          sql: 'amount',
        },
        visitor_revenue: {
          type: 'sum',
          sql: 'amount',
          filters: [{
            sql: \`\${CUBE}.source = 'some'\`
          }]
        },
        per_visitor_revenue: perVisitorRevenueMeasure,
        revenueRunning: {
          type: 'runningTotal',
          sql: 'amount'
        },
        revenueRolling: {
          type: 'sum',
          sql: 'amount',
          rollingWindow: {
            trailing: '2 day',
            offset: 'start'
          }
        },
        revenueRollingThreeDay: {
          type: 'sum',
          sql: 'amount',
          rollingWindow: {
            trailing: '3 day',
            offset: 'start'
          }
        },
        countRolling: {
          type: 'count',
          rollingWindow: {
            trailing: '2 day',
            offset: 'start'
          }
        },
        countRollingThreeMonth: {
          type: 'count',
          rollingWindow: {
            trailing: '3 month',
            offset: 'end'
          }
        },
        countRollingUnbounded: {
          type: 'count',
          rollingWindow: {
            trailing: 'unbounded'
          }
        },
        countRollingWeekToDate: {
          type: 'count',
          rollingWindow: {
            type: 'to_date',
            granularity: 'week'
          }
        },
        countRollingThreeDaysToDate: {
          type: 'count',
          rollingWindow: {
            type: 'to_date',
            granularity: 'three_days'
          }
        },
        revenue_qtd: {
          type: 'sum',
          sql: 'amount',
          rollingWindow: {
            type: 'to_date',
            granularity: 'quarter'
          }
        },
        revenue_qtd_proxy: {
          type: 'sum',
          sql: \`\${revenue}\`,
          multi_stage: true,
          rollingWindow: {
            type: 'to_date',
            granularity: 'quarter'
          }
        },
        revenue_day_ago: {
          multi_stage: true,
          type: 'sum',
          sql: \`\${revenue}\`,
          time_shift: [{
            time_dimension: created_at,
            interval: '1 day',
            type: 'prior',
          }]
        },
        revenueRollingDayAgo: {
          type: 'sum',
          sql: \`\${revenue_day_ago}\`,
          multi_stage: true,
          rollingWindow: {
            trailing: '2 day',
            offset: 'start'
          }
        },
        revenue_day_ago_no_td: {
          multi_stage: true,
          type: 'sum',
          sql: \`\${revenue}\`,
          time_shift: [{
            interval: '1 day',
            type: 'prior',
          }]
        },
        revenue_day_ago_via_join: {
          multi_stage: true,
          type: 'sum',
          sql: \`\${revenue}\`,
          time_shift: [{
            time_dimension: visitors_create_dates.create_date_created_at,
            interval: '1 day',
            type: 'prior',
          }]
        },
        cagr_day: {
          multi_stage: true,
          sql: \`ROUND(100 * \${revenue} / NULLIF(\${revenue_day_ago}, 0))\`,
          type: 'number',
        },
        countDistinctApproxRolling: {
          type: 'countDistinctApprox',
          sql: 'id',
          rollingWindow: {
            trailing: '2 day',
            offset: 'start'
          }
        },
        runningCount: {
          type: 'runningTotal',
          sql: '1'
        },
        runningRevenuePerCount: {
          type: 'number',
          sql: \`round(\${revenueRunning} / \${runningCount})\`
        },
        averageCheckins: {
          type: 'avg',
          sql: \`\${doubledCheckings}\`
        },
        strCase: {
          sql: \`CASE WHEN \${visitor_count} > 1 THEN 'More than 1' ELSE (\${visitor_revenue})::text END\`,
          type: \`string\`
        },
        unique_sources: {
          type: 'countDistinct',
          sql: \`source\`
        },
        ...(['foo', 'bar'].map(m => ({ [m]: { type: 'count' } })).reduce((a, b) => ({ ...a, ...b }))),
        second_rank_sum: {
          multi_stage: true,
          sql: \`\${visitor_revenue}\`,
          filters: [{
            sql: \`\${revenue_rank} = 1\`
          }],
          type: 'sum',
        },
        adjusted_rank_sum: {
          multi_stage: true,
          sql: \`\${adjusted_revenue}\`,
          filters: [{
            sql: \`\${adjusted_revenue_rank} = 1\`
          }],
          type: 'sum',
          add_group_by: [visitors.created_at],
        },
        revenue_sum_group_by_granularity: {
          multi_stage: true,
          sql: \`\${revenue}\`,
          type: 'number',
          add_group_by: [visitors.created_at.month],
        },
        revenue_rank: {
          multi_stage: true,
          type: \`rank\`,
          order_by: [{
            sql: \`\${visitor_revenue}\`,
            dir: 'asc'
          }],
          reduce_by: [visitors.source],
        },
        date_rank: {
          multi_stage: true,
          type: \`rank\`,
          order_by: [{
            sql: \`\${visitors.created_at}\`,
            dir: 'asc'
          }],
          reduce_by: [visitors.created_at]
        },
        adjusted_revenue_rank: {
          multi_stage: true,
          type: \`rank\`,
          order_by: [{
            sql: \`\${adjusted_revenue}\`,
            dir: 'asc'
          }],
          reduce_by: [visitors.created_at]
        },
        visitors_revenue_total: {
          multi_stage: true,
          sql: \`\${revenue}\`,
          type: 'sum',
          group_by: []
        },
        visitors_revenue_per_source: {
          multi_stage: true,
          sql: \`\${revenue}\`,
          type: 'sum',
          group_by: [visitors.source]
        },
        visitors_revenue_without_date: {
          multi_stage: true,
          sql: \`\${revenue}\`,
          type: 'sum',
          reduce_by: [visitors.created_at]
        },
        percentage_of_total: {
          multi_stage: true,
          sql: \`(100 * \${revenue} / NULLIF(\${visitors_revenue_total}, 0))::int\`,
          type: 'number'
        },
        adjusted_revenue: {
          multi_stage: true,
          sql: \`\${visitor_revenue} + 0.1 * \${date_rank}\`,
          type: 'number',
          filters: [{
            sql: \`\${date_rank} <= 3\`
          }]
        },
        customer_revenue: {
          multi_stage: true,
          sql: \`\${revenue}\`,
          type: 'sum',
          group_by: [id]
        },
        min_created_at: {
          type: 'time',
          sql: 'MIN(created_at)'
        }
      },

      dimensions: {
        revenue_bucket: {
          multi_stage: true,
          sql: \`CASE WHEN \${revenue} < 100 THEN 1 WHEN \${revenue} >= 100 THEN 2 END\`,
          type: 'number',
          add_group_by: [id]
        },
        id: {
          type: 'number',
          sql: 'id',
          primaryKey: true
        },
        source: {
          type: 'string',
          sql: 'source'
        },
        created_at: {
          type: 'time',
          sql: 'created_at',
          granularities: {
            three_days: {
              interval: '3 days',
              title: '3 days',
              origin: '2017-01-01'
            }
          }
        },
        created_month: {
          type: 'time',
          sql: \`\${created_at.month}\`
        },
        updated_at: {
          type: 'time',
          sql: 'updated_at'
        },

        createdAtSqlUtils: {
          type: 'time',
          sql: SQL_UTILS.convertTz('created_at')
        },

        checkins: {
          sql: \`\${visitor_checkins.visitor_checkins_count}\`,
          type: \`number\`,
          subQuery: true
        },

        checkinsRolling: {
          sql: \`\${visitor_checkins.visitorCheckinsRolling}\`,
          type: \`number\`,
          subQuery: true
        },

        checkinsWithPropagation: {
          sql: \`\${visitor_checkins.visitor_checkins_count}\`,
          type: \`number\`,
          subQuery: true,
          propagateFiltersToSubQuery: true
        },

        subQueryFail: {
          sql: '2',
          type: \`number\`,
          subQuery: true
        },

        doubledCheckings: {
          sql: \`\${checkins} * 2\`,
          type: 'number'
        },
        minVisitorCheckinDate: {
          sql: \`\${visitor_checkins.minDate}\`,
          type: 'time',
          subQuery: true
        },
        minVisitorCheckinDate1: {
          sql: \`\${visitor_checkins.minDate1}\`,
          type: 'time',
          subQuery: true
        },
        location: {
          type: \`geo\`,
          latitude: { sql: \`latitude\` },
          longitude: { sql: \`longitude\` }
        },
        questionMark: {
          sql: \`replace('some string question string ? ?? ???', 'string', 'with some ? ?? ???')\`,
          type: \`string\`
        }
      }
    });

    view('visitors_multi_stage', {
      cubes: [{
        join_path: 'visitors',
        includes: '*'
      },
      {
        join_path: 'visitors.visitors_create_dates',
        includes: '*'
      }]
    })

    cube('visitor_checkins', {
      sql: \`
      select visitor_checkins.* from visitor_checkins left join visitors on visitor_checkins.visitor_id = visitors.id WHERE
      \${FILTER_PARAMS.visitor_checkins.created_at.filter('visitor_checkins.created_at')} AND
      \${FILTER_GROUP(FILTER_PARAMS.visitor_checkins.created_at.filter("(visitor_checkins.created_at - INTERVAL '3 DAY')"), FILTER_PARAMS.visitor_checkins.source.filter('visitor_checkins.source'))}
      AND \${SECURITY_CONTEXT.source.filter('visitors.source')} AND
      \${SECURITY_CONTEXT.sourceArray.filter(sourceArray => \`visitors.source in (\${sourceArray.join(',')})\`)}

      \`,
      sql_alias: \`vc\`,

      rewriteQueries: true,

      joins: {
        cards: {
          relationship: 'hasMany',
          sql: \`\${CUBE}.id = \${cards}.visitor_checkin_id\`
        }
      },

      measures: {
        visitor_checkins_count: {
          type: 'count'
        },

        id_sum: {
          sql: 'id',
          type: 'sum'
        },

        visitorCheckinsRolling: {
          type: 'count',
          rollingWindow: {
            trailing: 'unbounded'
          }
        },

        revenue_per_checkin: {
          type: 'number',
          sql: \`\${visitors.visitor_revenue} / \${visitor_checkins_count}\`
        },
        google_sourced_checkins: {
          type: 'count',
          sql: 'id',
          filters: [{
            sql: \`\${visitors}.source = 'google'\`
          }]
        },
        unique_google_sourced_checkins: {
          type: 'countDistinct',
          sql: 'id',
          filters: [{
            sql: \`\${visitors}.source = 'google'\`
          }]
        },
        unique_sources_per_checking: {
          sql: \`\${visitors.unique_sources} / \${visitor_checkins_count}\`,
          type: 'number'
        },
        minDate: {
          type: 'min',
          sql: 'created_at'
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
        },
        cardsCount: {
          sql: \`\${cards.count}\`,
          type: \`number\`,
          subQuery: true
        },
      },

      preAggregations: {
        checkinSource: {
          type: 'rollup',
          measureReferences: [visitors.per_visitor_revenue],
          dimensionReferences: [visitor_checkins.source],
          timeDimensionReference: visitors.created_at,
          granularity: 'day'
        },
        visitorCountCheckinSource: {
          type: 'rollup',
          measureReferences: [visitors.visitor_revenue],
          dimensionReferences: [visitor_checkins.source],
          timeDimensionReference: visitors.created_at,
          granularity: 'day'
        }
      }
    });

    view('visitors_visitors_checkins_view', {
      cubes: [{
        join_path: 'visitors',
        includes: ['revenue', 'source', 'updated_at', 'visitor_revenue']
      }, {
        join_path: 'visitors.visitor_checkins',
        includes: ['visitor_checkins_count', 'id_sum']
      }]
    })

    cube('visitor_checkins_sources', {
      sql: \`
      select id, visitor_id, source from visitor_checkins WHERE \${FILTER_PARAMS.visitor_checkins_sources.source.filter('source')}
      \`,

      rewriteQueries: true,

      joins: {
        cards: {
          relationship: 'hasMany',
          sql: \`\${CUBE}.id = \${cards}.visitor_checkin_id\`
        }
      },

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
        }
      }
    })

    cube('cards', {
      sql: \`
      select * from cards
      \`,

      joins: {
        visitors: {
          relationship: 'belongsTo',
          sql: \`\${visitors}.id = \${cards}.visitor_id\`
        }
      },

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
        }
      }
    })

    cube('ReferenceVisitors', {
      sql: \`
        select * from \${visitors.sql()} as t
        WHERE \${FILTER_PARAMS.ReferenceVisitors.createdAt.filter(\`(t.created_at + interval '28 day')\`)} AND
        \${FILTER_PARAMS.ReferenceVisitors.createdAt.filter((from, to) => \`(t.created_at + interval '28 day') >= \${from} AND (t.created_at + interval '28 day') <= \${to}\`)}
      \`,

      measures: {
        count: {
          type: 'count'
        },

        googleSourcedCount: {
          type: 'count',
          filters: [{
            sql: \`\${CUBE}.source = 'google'\`
          }]
        },
      },

      dimensions: {
        createdAt: {
          type: 'time',
          sql: 'created_at'
        }
      }
    })

    cube('CubeWithVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryLongName', {
      sql: \`
      select * from cards
      \`,

      sqlAlias: 'cube_with_long_name',

      dataSource: 'oracle',

      measures: {
        count: {
          type: 'count'
        }
      }
    });

    cube('compound', {
      sql: \`
        select * from compound_key_cards
      \`,

      joins: {
        visitors: {
          relationship: 'belongsTo',
          sql: \`\${visitors}.id = \${CUBE}.visitor_id\`
        },
      },

      measures: {
        count: {
          type: 'count'
        },
        rank_avg: {
          type: 'avg',
          sql: 'visit_rank'
        }
      },
      dimensions: {
        id_a: {
          type: 'number',
          sql: 'id_a',
          primaryKey: true
        },
        id_b: {
          type: 'number',
          sql: 'id_b',
          primaryKey: true
        },
      }
    });

    cube('rollingWindowDates', {
      sql: \`
        SELECT cast('2024-01-13' AS timestamp) as time UNION ALL
        SELECT cast('2024-02-13' AS timestamp) as time UNION ALL
        SELECT cast('2024-03-13' AS timestamp) as time UNION ALL
        SELECT cast('2024-04-13' AS timestamp) as time UNION ALL
        SELECT cast('2024-05-13' AS timestamp) as time UNION ALL
        SELECT cast('2024-06-13' AS timestamp) as time UNION ALL
        SELECT cast('2024-07-13' AS timestamp) as time UNION ALL
        SELECT cast('2024-08-13' AS timestamp) as time UNION ALL
        SELECT cast('2024-09-13' AS timestamp) as time UNION ALL
        SELECT cast('2024-10-13' AS timestamp) as time UNION ALL
        SELECT cast('2024-11-13' AS timestamp) as time UNION ALL
        SELECT cast('2024-12-13' AS timestamp) as time
      \`,

      dimensions: {
        time: {
          type: 'time',
          sql: 'time',
          primaryKey: true
        }
      }
    });

    cube('rollingWindowTest', {
      sql: \`
SELECT 1 AS revenue,  cast('2024-01-01' AS timestamp) as time UNION ALL
      SELECT 1 AS revenue,  cast('2024-02-01' AS timestamp) as time UNION ALL
      SELECT 1 AS revenue,  cast('2024-03-01' AS timestamp) as time UNION ALL
      SELECT 1 AS revenue,  cast('2024-04-01' AS timestamp) as time UNION ALL
      SELECT 1 AS revenue,  cast('2024-05-01' AS timestamp) as time UNION ALL
      SELECT 1 AS revenue,  cast('2024-06-01' AS timestamp) as time UNION ALL
      SELECT 1 AS revenue,  cast('2024-07-01' AS timestamp) as time UNION ALL
      SELECT 1 AS revenue,  cast('2024-08-01' AS timestamp) as time UNION ALL
      SELECT 1 AS revenue,  cast('2024-09-01' AS timestamp) as time UNION ALL
      SELECT 1 AS revenue,  cast('2024-10-01' AS timestamp) as time UNION ALL
      SELECT 1 AS revenue,  cast('2024-11-01' AS timestamp) as time UNION ALL
      SELECT 1 AS revenue,  cast('2024-12-01' AS timestamp) as time
      \`,

      dimensions: {
        time: {
          type: 'time',
          sql: 'time',
          primaryKey: true
        }
      },
      measures: {
        revenue: {
          sql: 'revenue',
          type: 'sum',
          filters: [{
            sql: \`\${rollingWindowDates.time} <= current_date\`
          }]
        },
        revenue_ytd: {
          sql: \`\${CUBE.revenue}\`,
          type: 'sum',
          rolling_window: {
            type: 'to_date',
            granularity: 'year'
          }
        },
        revenue_ms: {
          sql: \`\${CUBE.revenue}\`,
          type: 'sum',
          multi_stage: true,
        },
      },
      joins: {
        rollingWindowDates: {
          relationship: 'manyToOne',
          sql: \`\${CUBE}.time = date_trunc('month', \${rollingWindowDates.time})\`
        }
      }
    });

    cube('UngroupedMeasureWithFilter1', {
      sql: \`
        SELECT
          1 AS id,
          1 AS sum
      \`,
      sqlAlias: 'umwf1',
      dimensions: {
        id: {
          sql: \`id\`,
          type: 'number',
          primaryKey: true
        }
      },
      measures: {
        count: {
          type: 'count',
        },
        sum_filter: {
          sql: \`sum\`,
          type: 'sum',
          filters: [{sql: \`\${UngroupedMeasureWithFilter3.id} = 1\`}]
        }
      },
      joins: {
        UngroupedMeasureWithFilter2: {
          sql: \`\${CUBE.id} = \${UngroupedMeasureWithFilter2.id}\`,
          relationship: \`one_to_many\`
        },
        UngroupedMeasureWithFilter3: {
          sql: \`\${CUBE.id} = \${UngroupedMeasureWithFilter3.id}\`,
          relationship: \`many_to_one\`
        }
      }
    });

    cube('UngroupedMeasureWithFilter2', {
      sql: \`
        SELECT
          1 AS id
      \`,
      sqlAlias: 'umwf2',
      dimensions: {
        id: {
          sql: \`id\`,
          type: 'number',
          primaryKey: true
        }
      },
      measures: {
        count: {
          type: 'count',
        }
      }
    });

    cube('UngroupedMeasureWithFilter3', {
      sql: \`
        SELECT
          1 AS id
      \`,
      sqlAlias: 'umwf3',
      dimensions: {
        id: {
          sql: \`id\`,
          type: 'number',
          primaryKey: true
        }
      },
      measures: {
        count: {
          type: 'count',
        }
      }
    });

    view('UngroupedMeasureWithFilter_View', {
      cubes: [{
        join_path: 'UngroupedMeasureWithFilter1',
        includes: ['sum_filter']
      }, {
        join_path: 'UngroupedMeasureWithFilter1.UngroupedMeasureWithFilter2',
        includes: ['count']
      }]
    })
    `);

  it('simple join', async () => {
    await compiler.compile();

    console.log(joinGraph.buildJoin(['visitor_checkins', 'visitors']));

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors.visitor_revenue',
        'visitors.visitor_count',
        'visitor_checkins.visitor_checkins_count',
        'visitors.per_visitor_revenue'
      ],
      timeDimensions: [{
        dimension: 'visitors.created_at',
        granularity: 'day',
        dateRange: ['2017-01-01', '2017-01-30']
      }],
      timezone: 'America/Los_Angeles',
      order: [{
        id: 'visitors.created_at'
      }]
    });

    return dbRunner.testQuery(query.buildSqlAndParams()).then(res => {
      expect(res).toEqual(
        [
          {
            visitors__created_at_day: '2017-01-02T00:00:00.000Z',
            visitors__visitor_revenue: '100',
            visitors__visitor_count: '1',
            vc__visitor_checkins_count: '3',
            visitors__per_visitor_revenue: '100'
          },
          {
            visitors__created_at_day: '2017-01-04T00:00:00.000Z',
            visitors__visitor_revenue: '200',
            visitors__visitor_count: '1',
            vc__visitor_checkins_count: '2',
            visitors__per_visitor_revenue: '200'
          },
          {
            visitors__created_at_day: '2017-01-05T00:00:00.000Z',
            visitors__visitor_revenue: null,
            visitors__visitor_count: '1',
            vc__visitor_checkins_count: '1',
            visitors__per_visitor_revenue: null
          },
          {
            visitors__created_at_day: '2017-01-06T00:00:00.000Z',
            visitors__visitor_revenue: null,
            visitors__visitor_count: '2',
            vc__visitor_checkins_count: '0',
            visitors__per_visitor_revenue: null
          }
        ]
      );
    });
  });

  async function runQueryTest(q, expectedResult) {
    await compiler.compile();
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, q);
    const res = await dbRunner.testQuery(query.buildSqlAndParams());
    console.log(JSON.stringify(res));

    expect(res).toEqual(
      expectedResult
    );
  }

  it('simple join total', async () => runQueryTest({
    measures: [
      'visitors.visitor_revenue',
      'visitors.visitor_count',
      'visitor_checkins.visitor_checkins_count',
      'visitors.per_visitor_revenue'
    ],
    timeDimensions: [{
      dimension: 'visitors.created_at',
      dateRange: ['2017-01-01', '2017-01-30']
    }],
    timezone: 'America/Los_Angeles',
    order: []
  }, [{
    visitors__visitor_revenue: '300',
    visitors__visitor_count: '5',
    vc__visitor_checkins_count: '6',
    visitors__per_visitor_revenue: '60'
  }]));

  it('string measure', async () => runQueryTest({
    measures: [
      'visitors.strCase',
    ],
    timeDimensions: [{
      dimension: 'visitors.created_at',
      dateRange: ['2017-01-01', '2017-01-30']
    }],
    timezone: 'America/Los_Angeles',
    order: []
  }, [{
    visitors__str_case: 'More than 1'
  }]));

  it('running total', async () => {
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors.revenueRunning'
      ],
      timeDimensions: [{
        dimension: 'visitors.created_at',
        granularity: 'day',
        dateRange: ['2017-01-01', '2017-01-10']
      }],
      order: [{
        id: 'visitors.created_at'
      }],
      timezone: 'America/Los_Angeles'
    });

    console.log(query.buildSqlAndParams());

    // TODO ordering doesn't work for running total
    return dbRunner.testQuery(query.buildSqlAndParams()).then(res => {
      console.log(JSON.stringify(res));
      expect(res).toEqual(
        [{
          visitors__created_at_day: '2017-01-01T00:00:00.000Z',
          visitors__revenue_running: null
        }, {
          visitors__created_at_day: '2017-01-02T00:00:00.000Z',
          visitors__revenue_running: '100'
        }, {
          visitors__created_at_day: '2017-01-03T00:00:00.000Z',
          visitors__revenue_running: '100'
        }, {
          visitors__created_at_day: '2017-01-04T00:00:00.000Z',
          visitors__revenue_running: '300'
        }, {
          visitors__created_at_day: '2017-01-05T00:00:00.000Z',
          visitors__revenue_running: '600'
        }, {
          visitors__created_at_day: '2017-01-06T00:00:00.000Z',
          visitors__revenue_running: '1500'
        }, {
          visitors__created_at_day: '2017-01-07T00:00:00.000Z',
          visitors__revenue_running: '1500'
        }, {
          visitors__created_at_day: '2017-01-08T00:00:00.000Z',
          visitors__revenue_running: '1500'
        }, {
          visitors__created_at_day: '2017-01-09T00:00:00.000Z',
          visitors__revenue_running: '1500'
        }, {
          visitors__created_at_day: '2017-01-10T00:00:00.000Z',
          visitors__revenue_running: '1500'
        }]
      );
    });
  });

  it('rolling', async () => runQueryTest({
    measures: [
      'visitors.revenueRolling'
    ],
    timeDimensions: [{
      dimension: 'visitors.created_at',
      granularity: 'day',
      dateRange: ['2017-01-01', '2017-01-10']
    }],
    order: [{
      id: 'visitors.created_at'
    }],
    timezone: 'America/Los_Angeles'
  }, [
    { visitors__created_at_day: '2017-01-01T00:00:00.000Z', visitors__revenue_rolling: null },
    { visitors__created_at_day: '2017-01-02T00:00:00.000Z', visitors__revenue_rolling: null },
    { visitors__created_at_day: '2017-01-03T00:00:00.000Z', visitors__revenue_rolling: '100' },
    { visitors__created_at_day: '2017-01-04T00:00:00.000Z', visitors__revenue_rolling: '100' },
    { visitors__created_at_day: '2017-01-05T00:00:00.000Z', visitors__revenue_rolling: '200' },
    { visitors__created_at_day: '2017-01-06T00:00:00.000Z', visitors__revenue_rolling: '500' },
    { visitors__created_at_day: '2017-01-07T00:00:00.000Z', visitors__revenue_rolling: '1200' },
    { visitors__created_at_day: '2017-01-08T00:00:00.000Z', visitors__revenue_rolling: '900' },
    { visitors__created_at_day: '2017-01-09T00:00:00.000Z', visitors__revenue_rolling: null },
    { visitors__created_at_day: '2017-01-10T00:00:00.000Z', visitors__revenue_rolling: null }
  ]));

  if (getEnv('nativeSqlPlanner')) {
    it('rolling day ago', async () => runQueryTest({
      measures: [
        'visitors.revenueRollingDayAgo'
      ],
      timeDimensions: [{
        dimension: 'visitors.created_at',
        granularity: 'day',
        dateRange: ['2017-01-01', '2017-01-10']
      }],
      order: [{
        id: 'visitors.created_at'
      }],
      timezone: 'America/Los_Angeles'
    }, [
      { visitors__created_at_day: '2017-01-01T00:00:00.000Z', visitors__revenue_rolling_day_ago: null },
      { visitors__created_at_day: '2017-01-02T00:00:00.000Z', visitors__revenue_rolling_day_ago: null },
      { visitors__created_at_day: '2017-01-03T00:00:00.000Z', visitors__revenue_rolling_day_ago: null },
      { visitors__created_at_day: '2017-01-04T00:00:00.000Z', visitors__revenue_rolling_day_ago: '100' },
      { visitors__created_at_day: '2017-01-05T00:00:00.000Z', visitors__revenue_rolling_day_ago: '100' },
      { visitors__created_at_day: '2017-01-06T00:00:00.000Z', visitors__revenue_rolling_day_ago: '200' },
      { visitors__created_at_day: '2017-01-07T00:00:00.000Z', visitors__revenue_rolling_day_ago: '500' },
      { visitors__created_at_day: '2017-01-08T00:00:00.000Z', visitors__revenue_rolling_day_ago: '1200' },
      { visitors__created_at_day: '2017-01-09T00:00:00.000Z', visitors__revenue_rolling_day_ago: '900' },
      { visitors__created_at_day: '2017-01-10T00:00:00.000Z', visitors__revenue_rolling_day_ago: null }
    ]));
  } else {
    it.skip('rolling count without date range', () => {
      // Skipping because it works only in Tesseract
    });
  }

  it('rolling multiplied', async () => runQueryTest({
    measures: [
      'visitors.revenueRolling',
      'visitor_checkins.visitor_checkins_count'
    ],
    timeDimensions: [{
      dimension: 'visitors.created_at',
      granularity: 'day',
      dateRange: ['2017-01-01', '2017-01-10']
    }],
    order: [{
      id: 'visitors.created_at'
    }],
    timezone: 'America/Los_Angeles'
  },
  getEnv('nativeSqlPlanner') ?
    [
      {
        vc__visitor_checkins_count: null,
        visitors__created_at_day: '2017-01-01T00:00:00.000Z',
        visitors__revenue_rolling: null,
      },
      {
        visitors__created_at_day: '2017-01-02T00:00:00.000Z',
        visitors__revenue_rolling: null,
        vc__visitor_checkins_count: '3'
      },
      {

        vc__visitor_checkins_count: null,
        visitors__created_at_day: '2017-01-03T00:00:00.000Z',
        visitors__revenue_rolling: '100',
      },
      {
        visitors__created_at_day: '2017-01-04T00:00:00.000Z',
        visitors__revenue_rolling: '100',
        vc__visitor_checkins_count: '2'
      },
      {
        visitors__created_at_day: '2017-01-05T00:00:00.000Z',
        visitors__revenue_rolling: '200',
        vc__visitor_checkins_count: '1'
      },
      {
        visitors__created_at_day: '2017-01-06T00:00:00.000Z',
        visitors__revenue_rolling: '500',
        vc__visitor_checkins_count: '0'
      },
      {
        vc__visitor_checkins_count: null,
        visitors__created_at_day: '2017-01-07T00:00:00.000Z',
        visitors__revenue_rolling: '1200',
      },
      {
        vc__visitor_checkins_count: null,
        visitors__created_at_day: '2017-01-08T00:00:00.000Z',
        visitors__revenue_rolling: '900',
      },
      {
        vc__visitor_checkins_count: null,
        visitors__created_at_day: '2017-01-09T00:00:00.000Z',
        visitors__revenue_rolling: null,
      },
      {
        vc__visitor_checkins_count: null,
        visitors__created_at_day: '2017-01-10T00:00:00.000Z',
        visitors__revenue_rolling: null,
      }
    ]
    :
    [
      {
        visitors__created_at_day: '2017-01-02T00:00:00.000Z',
        visitors__revenue_rolling: null,
        vc__visitor_checkins_count: '3'
      },
      {
        visitors__created_at_day: '2017-01-04T00:00:00.000Z',
        visitors__revenue_rolling: '100',
        vc__visitor_checkins_count: '2'
      },
      {
        visitors__created_at_day: '2017-01-05T00:00:00.000Z',
        visitors__revenue_rolling: '200',
        vc__visitor_checkins_count: '1'
      },
      {
        visitors__created_at_day: '2017-01-06T00:00:00.000Z',
        visitors__revenue_rolling: '500',
        vc__visitor_checkins_count: '0'
      }
    ]));

  it('rolling window with one time dimension with granularity', async () => runQueryTest({
    measures: [
      'visitors.countRollingWeekToDate'
    ],
    timeDimensions: [
      {
        dimension: 'visitors.created_at',
        granularity: 'day',
        dateRange: ['2017-01-01', '2017-01-10']
      }
    ],
    order: [{
      id: 'visitors.created_at'
    }],
    timezone: 'America/Los_Angeles'
  }, [
    {
      visitors__count_rolling_week_to_date: null,
      visitors__created_at_day: '2017-01-01T00:00:00.000Z',
    },
    {
      visitors__count_rolling_week_to_date: '1',
      visitors__created_at_day: '2017-01-02T00:00:00.000Z',
    },
    {
      visitors__count_rolling_week_to_date: '1',
      visitors__created_at_day: '2017-01-03T00:00:00.000Z',
    },
    {
      visitors__count_rolling_week_to_date: '2',
      visitors__created_at_day: '2017-01-04T00:00:00.000Z',
    },
    {
      visitors__count_rolling_week_to_date: '3',
      visitors__created_at_day: '2017-01-05T00:00:00.000Z',
    },
    {
      visitors__count_rolling_week_to_date: '5',
      visitors__created_at_day: '2017-01-06T00:00:00.000Z',
    },
    {
      visitors__count_rolling_week_to_date: '5',
      visitors__created_at_day: '2017-01-07T00:00:00.000Z',
    },
    {
      visitors__count_rolling_week_to_date: '5',
      visitors__created_at_day: '2017-01-08T00:00:00.000Z',
    },
    {
      visitors__count_rolling_week_to_date: null,
      visitors__created_at_day: '2017-01-09T00:00:00.000Z',
    },
    {
      visitors__count_rolling_week_to_date: null,
      visitors__created_at_day: '2017-01-10T00:00:00.000Z',
    },
  ]));

  it('rolling window with one time dimension without granularity', async () => runQueryTest({
    measures: [
      'visitors.countRollingWeekToDate'
    ],
    timeDimensions: [
      {
        dimension: 'visitors.created_at',
        dateRange: ['2017-01-01', '2017-01-10']
      }
    ],
    order: [{
      id: 'visitors.created_at'
    }],
    timezone: 'America/Los_Angeles'
  }, [
    {
      visitors__count_rolling_week_to_date: '5',
    }
  ]));

  it('rolling window with two time dimension granularities', async () => runQueryTest({
    measures: [
      'visitors.countRollingWeekToDate'
    ],
    timeDimensions: [
      {
        dimension: 'visitors.created_at',
        granularity: 'month',
        dateRange: ['2017-01-01', '2017-01-10']
      },
      {
        dimension: 'visitors.created_at',
        granularity: 'day',
        dateRange: ['2017-01-01', '2017-01-10']
      }
    ],
    order: [{
      id: 'visitors.created_at'
    }],
    timezone: 'America/Los_Angeles'
  }, [
    {
      visitors__count_rolling_week_to_date: null,
      visitors__created_at_day: '2017-01-01T00:00:00.000Z',
      visitors__created_at_month: '2017-01-01T00:00:00.000Z',
    },
    {
      visitors__count_rolling_week_to_date: '1',
      visitors__created_at_day: '2017-01-02T00:00:00.000Z',
      visitors__created_at_month: '2017-01-01T00:00:00.000Z',
    },
    {
      visitors__count_rolling_week_to_date: '1',
      visitors__created_at_day: '2017-01-03T00:00:00.000Z',
      visitors__created_at_month: '2017-01-01T00:00:00.000Z',
    },
    {
      visitors__count_rolling_week_to_date: '2',
      visitors__created_at_day: '2017-01-04T00:00:00.000Z',
      visitors__created_at_month: '2017-01-01T00:00:00.000Z',
    },
    {
      visitors__count_rolling_week_to_date: '3',
      visitors__created_at_day: '2017-01-05T00:00:00.000Z',
      visitors__created_at_month: '2017-01-01T00:00:00.000Z',
    },
    {
      visitors__count_rolling_week_to_date: '5',
      visitors__created_at_day: '2017-01-06T00:00:00.000Z',
      visitors__created_at_month: '2017-01-01T00:00:00.000Z',
    },
    {
      visitors__count_rolling_week_to_date: '5',
      visitors__created_at_day: '2017-01-07T00:00:00.000Z',
      visitors__created_at_month: '2017-01-01T00:00:00.000Z',
    },
    {
      visitors__count_rolling_week_to_date: '5',
      visitors__created_at_day: '2017-01-08T00:00:00.000Z',
      visitors__created_at_month: '2017-01-01T00:00:00.000Z',
    },
    {
      visitors__count_rolling_week_to_date: null,
      visitors__created_at_day: '2017-01-09T00:00:00.000Z',
      visitors__created_at_month: '2017-01-01T00:00:00.000Z',
    },
    {
      visitors__count_rolling_week_to_date: null,
      visitors__created_at_day: '2017-01-10T00:00:00.000Z',
      visitors__created_at_month: '2017-01-01T00:00:00.000Z',
    }
  ]));

  it('rolling window with two time dimension granularities one custom one regular', async () => runQueryTest({

    measures: [
      'visitors.countRollingWeekToDate'
    ],
    timeDimensions: [
      {
        dimension: 'visitors.created_at',
        granularity: 'three_days',
        dateRange: ['2017-01-01', '2017-01-10']
      },
      {
        dimension: 'visitors.created_at',
        granularity: 'day',
        dateRange: ['2017-01-01', '2017-01-10']
      }
    ],
    order: [{
      id: 'visitors.created_at'
    }],
    timezone: 'America/Los_Angeles'
  }, [
    {
      visitors__count_rolling_week_to_date: null,
      visitors__created_at_day: '2017-01-01T00:00:00.000Z',
      visitors__created_at_three_days: '2017-01-01T00:00:00.000Z',
    },
    {
      visitors__count_rolling_week_to_date: '1',
      visitors__created_at_day: '2017-01-02T00:00:00.000Z',
      visitors__created_at_three_days: '2017-01-01T00:00:00.000Z',
    },
    {
      visitors__count_rolling_week_to_date: '1',
      visitors__created_at_day: '2017-01-03T00:00:00.000Z',
      visitors__created_at_three_days: '2017-01-01T00:00:00.000Z',
    },
    {
      visitors__count_rolling_week_to_date: '2',
      visitors__created_at_day: '2017-01-04T00:00:00.000Z',
      visitors__created_at_three_days: '2017-01-04T00:00:00.000Z',
    },
    {
      visitors__count_rolling_week_to_date: '3',
      visitors__created_at_day: '2017-01-05T00:00:00.000Z',
      visitors__created_at_three_days: '2017-01-04T00:00:00.000Z',
    },
    {
      visitors__count_rolling_week_to_date: '5',
      visitors__created_at_day: '2017-01-06T00:00:00.000Z',
      visitors__created_at_three_days: '2017-01-04T00:00:00.000Z',
    },
    {
      visitors__count_rolling_week_to_date: '5',
      visitors__created_at_day: '2017-01-07T00:00:00.000Z',
      visitors__created_at_three_days: '2017-01-07T00:00:00.000Z',
    },
    {
      visitors__count_rolling_week_to_date: '5',
      visitors__created_at_day: '2017-01-08T00:00:00.000Z',
      visitors__created_at_three_days: '2017-01-07T00:00:00.000Z',
    },
    {
      visitors__count_rolling_week_to_date: null,
      visitors__created_at_day: '2017-01-09T00:00:00.000Z',
      visitors__created_at_three_days: '2017-01-07T00:00:00.000Z',
    },
    {
      visitors__count_rolling_week_to_date: null,
      visitors__created_at_day: '2017-01-10T00:00:00.000Z',
      visitors__created_at_three_days: '2017-01-10T00:00:00.000Z',
    }
  ]));

  if (getEnv('nativeSqlPlanner')) {
    it('custom granularity rolling window to_date with one time dimension with regular granularity', async () => runQueryTest({
      measures: [
        'visitors.countRollingThreeDaysToDate'
      ],
      timeDimensions: [
        {
          dimension: 'visitors.created_at',
          granularity: 'day',
          dateRange: ['2017-01-01', '2017-01-10']
        }
      ],
      order: [{
        id: 'visitors.created_at'
      }],
      timezone: 'America/Los_Angeles'
    }, [
      {
        visitors__count_rolling_three_days_to_date: null,
        visitors__created_at_day: '2017-01-01T00:00:00.000Z',
      },
      {
        visitors__count_rolling_three_days_to_date: '1',
        visitors__created_at_day: '2017-01-02T00:00:00.000Z',
      },
      {
        visitors__count_rolling_three_days_to_date: '1',
        visitors__created_at_day: '2017-01-03T00:00:00.000Z',
      },
      {
        visitors__count_rolling_three_days_to_date: '1',
        visitors__created_at_day: '2017-01-04T00:00:00.000Z',
      },
      {
        visitors__count_rolling_three_days_to_date: '2',
        visitors__created_at_day: '2017-01-05T00:00:00.000Z',
      },
      {
        visitors__count_rolling_three_days_to_date: '4',
        visitors__created_at_day: '2017-01-06T00:00:00.000Z',
      },
      {
        visitors__count_rolling_three_days_to_date: null,
        visitors__created_at_day: '2017-01-07T00:00:00.000Z',
      },
      {
        visitors__count_rolling_three_days_to_date: null,
        visitors__created_at_day: '2017-01-08T00:00:00.000Z',
      },
      {
        visitors__count_rolling_three_days_to_date: null,
        visitors__created_at_day: '2017-01-09T00:00:00.000Z',
      },
      {
        visitors__count_rolling_three_days_to_date: null,
        visitors__created_at_day: '2017-01-10T00:00:00.000Z',
      },
    ]));
  } else {
    it.skip('NO_BASE_QUERY_SUPPORT: custom granularity rolling window to_date with one time dimension with regular granularity', () => {
      // Skipping because it works only in Tesseract
    });
  }

  if (getEnv('nativeSqlPlanner')) {
    it('custom granularity rolling window to_date with two time dimension granularities one custom one regular', async () => runQueryTest({
      measures: [
        'visitors.countRollingThreeDaysToDate'
      ],
      timeDimensions: [
        {
          dimension: 'visitors.created_at',
          granularity: 'three_days',
          dateRange: ['2017-01-01', '2017-01-10']
        },
        {
          dimension: 'visitors.created_at',
          granularity: 'day',
          dateRange: ['2017-01-01', '2017-01-10']
        }
      ],
      order: [{
        id: 'visitors.created_at'
      }],
      timezone: 'America/Los_Angeles'
    }, [
      {
        visitors__count_rolling_three_days_to_date: null,
        visitors__created_at_day: '2017-01-01T00:00:00.000Z',
        visitors__created_at_three_days: '2017-01-01T00:00:00.000Z',
      },
      {
        visitors__count_rolling_three_days_to_date: '1',
        visitors__created_at_day: '2017-01-02T00:00:00.000Z',
        visitors__created_at_three_days: '2017-01-01T00:00:00.000Z',
      },
      {
        visitors__count_rolling_three_days_to_date: '1',
        visitors__created_at_day: '2017-01-03T00:00:00.000Z',
        visitors__created_at_three_days: '2017-01-01T00:00:00.000Z',
      },
      {
        visitors__count_rolling_three_days_to_date: '1',
        visitors__created_at_day: '2017-01-04T00:00:00.000Z',
        visitors__created_at_three_days: '2017-01-04T00:00:00.000Z',
      },
      {
        visitors__count_rolling_three_days_to_date: '2',
        visitors__created_at_day: '2017-01-05T00:00:00.000Z',
        visitors__created_at_three_days: '2017-01-04T00:00:00.000Z',
      },
      {
        visitors__count_rolling_three_days_to_date: '4',
        visitors__created_at_day: '2017-01-06T00:00:00.000Z',
        visitors__created_at_three_days: '2017-01-04T00:00:00.000Z',
      },
      {
        visitors__count_rolling_three_days_to_date: null,
        visitors__created_at_day: '2017-01-07T00:00:00.000Z',
        visitors__created_at_three_days: '2017-01-07T00:00:00.000Z',
      },
      {
        visitors__count_rolling_three_days_to_date: null,
        visitors__created_at_day: '2017-01-08T00:00:00.000Z',
        visitors__created_at_three_days: '2017-01-07T00:00:00.000Z',
      },
      {
        visitors__count_rolling_three_days_to_date: null,
        visitors__created_at_day: '2017-01-09T00:00:00.000Z',
        visitors__created_at_three_days: '2017-01-07T00:00:00.000Z',
      },
      {
        visitors__count_rolling_three_days_to_date: null,
        visitors__created_at_day: '2017-01-10T00:00:00.000Z',
        visitors__created_at_three_days: '2017-01-10T00:00:00.000Z',
      },
    ]));
  } else {
    it.skip('NO_BASE_QUERY_SUPPORT: custom granularity rolling window to_date with two time dimension granularities one custom one regular', () => {
      // Skipping because it works only in Tesseract
    });
  }

  it('rolling window with same td with and without granularity', async () => runQueryTest({
    measures: [
      'visitors.countRollingWeekToDate'
    ],
    timeDimensions: [
      {
        dimension: 'visitors.created_at',
        granularity: 'day',
        dateRange: ['2017-01-01', '2017-01-10']
      },
      {
        dimension: 'visitors.created_at',
        dateRange: ['2017-01-01', '2017-01-10']
      }
    ],
    order: [{
      id: 'visitors.created_at'
    }],
    timezone: 'America/Los_Angeles'
  }, [{
    visitors__count_rolling_week_to_date: null,
    visitors__created_at_day: '2017-01-01T00:00:00.000Z',
  },
  {
    visitors__count_rolling_week_to_date: '1',
    visitors__created_at_day: '2017-01-02T00:00:00.000Z',
  },
  {
    visitors__count_rolling_week_to_date: '1',
    visitors__created_at_day: '2017-01-03T00:00:00.000Z',
  },
  {
    visitors__count_rolling_week_to_date: '2',
    visitors__created_at_day: '2017-01-04T00:00:00.000Z',
  },
  {
    visitors__count_rolling_week_to_date: '3',
    visitors__created_at_day: '2017-01-05T00:00:00.000Z',
  },
  {
    visitors__count_rolling_week_to_date: '5',
    visitors__created_at_day: '2017-01-06T00:00:00.000Z',
  },
  {
    visitors__count_rolling_week_to_date: '5',
    visitors__created_at_day: '2017-01-07T00:00:00.000Z',
  },
  {
    visitors__count_rolling_week_to_date: '5',
    visitors__created_at_day: '2017-01-08T00:00:00.000Z',
  },
  {
    visitors__count_rolling_week_to_date: null,
    visitors__created_at_day: '2017-01-09T00:00:00.000Z',
  },
  {
    visitors__count_rolling_week_to_date: null,
    visitors__created_at_day: '2017-01-10T00:00:00.000Z',
  }]));

  it('two rolling windows with two time dimension granularities', async () => runQueryTest({
    measures: [
      'visitors.countRollingUnbounded',
      'visitors.countRollingWeekToDate'
    ],
    timeDimensions: [
      {
        dimension: 'visitors.created_at',
        granularity: 'day',
        dateRange: ['2017-01-01', '2017-01-10']
      },
      {
        dimension: 'visitors.created_at',
        granularity: 'week',
        dateRange: ['2017-01-01', '2017-01-10']
      }
    ],
    order: [{
      id: 'visitors.created_at'
    }],
    timezone: 'America/Los_Angeles'
  }, [
    {
      visitors__count_rolling_unbounded: '1',
      visitors__count_rolling_week_to_date: null,
      visitors__created_at_day: '2017-01-01T00:00:00.000Z',
      visitors__created_at_week: '2016-12-26T00:00:00.000Z',
    },
    {
      visitors__count_rolling_unbounded: '2',
      visitors__count_rolling_week_to_date: '1',
      visitors__created_at_day: '2017-01-02T00:00:00.000Z',
      visitors__created_at_week: '2017-01-02T00:00:00.000Z',
    },
    {
      visitors__count_rolling_unbounded: '2',
      visitors__count_rolling_week_to_date: '1',
      visitors__created_at_day: '2017-01-03T00:00:00.000Z',
      visitors__created_at_week: '2017-01-02T00:00:00.000Z',
    },
    {
      visitors__count_rolling_unbounded: '3',
      visitors__count_rolling_week_to_date: '2',
      visitors__created_at_day: '2017-01-04T00:00:00.000Z',
      visitors__created_at_week: '2017-01-02T00:00:00.000Z',
    },
    {
      visitors__count_rolling_unbounded: '4',
      visitors__count_rolling_week_to_date: '3',
      visitors__created_at_day: '2017-01-05T00:00:00.000Z',
      visitors__created_at_week: '2017-01-02T00:00:00.000Z',
    },
    {
      visitors__count_rolling_unbounded: '6',
      visitors__count_rolling_week_to_date: '5',
      visitors__created_at_day: '2017-01-06T00:00:00.000Z',
      visitors__created_at_week: '2017-01-02T00:00:00.000Z',
    },
    {
      visitors__count_rolling_unbounded: '6',
      visitors__count_rolling_week_to_date: '5',
      visitors__created_at_day: '2017-01-07T00:00:00.000Z',
      visitors__created_at_week: '2017-01-02T00:00:00.000Z',
    },
    {
      visitors__count_rolling_unbounded: '6',
      visitors__count_rolling_week_to_date: '5',
      visitors__created_at_day: '2017-01-08T00:00:00.000Z',
      visitors__created_at_week: '2017-01-02T00:00:00.000Z',
    },
    {
      visitors__count_rolling_unbounded: '6',
      visitors__count_rolling_week_to_date: null,
      visitors__created_at_day: '2017-01-09T00:00:00.000Z',
      visitors__created_at_week: '2017-01-09T00:00:00.000Z',
    },
    {
      visitors__count_rolling_unbounded: '6',
      visitors__count_rolling_week_to_date: null,
      visitors__created_at_day: '2017-01-10T00:00:00.000Z',
      visitors__created_at_week: '2017-01-09T00:00:00.000Z',
    }
  ]));

  it('rolling month', async () => runQueryTest({
    measures: [
      'visitors.revenueRollingThreeDay'
    ],
    timeDimensions: [{
      dimension: 'visitors.created_at',
      granularity: 'week',
      dateRange: ['2017-01-09', '2017-01-10']
    }],
    order: [{
      id: 'visitors.created_at'
    }],
    timezone: 'America/Los_Angeles'
  }, [
    { visitors__created_at_week: '2017-01-09T00:00:00.000Z', visitors__revenue_rolling_three_day: '900' }
  ]));

  it('rolling count', async () => runQueryTest({
    measures: [
      'visitors.countRolling'
    ],
    timeDimensions: [{
      dimension: 'visitors.created_at',
      granularity: 'day',
      dateRange: ['2017-01-01', '2017-01-10']
    }],
    order: [{
      id: 'visitors.created_at'
    }],
    timezone: 'America/Los_Angeles'
  }, [
    { visitors__created_at_day: '2017-01-01T00:00:00.000Z', visitors__count_rolling: null },
    { visitors__created_at_day: '2017-01-02T00:00:00.000Z', visitors__count_rolling: null },
    { visitors__created_at_day: '2017-01-03T00:00:00.000Z', visitors__count_rolling: '1' },
    { visitors__created_at_day: '2017-01-04T00:00:00.000Z', visitors__count_rolling: '1' },
    { visitors__created_at_day: '2017-01-05T00:00:00.000Z', visitors__count_rolling: '1' },
    { visitors__created_at_day: '2017-01-06T00:00:00.000Z', visitors__count_rolling: '2' },
    { visitors__created_at_day: '2017-01-07T00:00:00.000Z', visitors__count_rolling: '3' },
    { visitors__created_at_day: '2017-01-08T00:00:00.000Z', visitors__count_rolling: '2' },
    { visitors__created_at_day: '2017-01-09T00:00:00.000Z', visitors__count_rolling: null },
    { visitors__created_at_day: '2017-01-10T00:00:00.000Z', visitors__count_rolling: null }
  ]));

  if (getEnv('nativeSqlPlanner')) {
    it('rolling count without date range', async () => {
      await runQueryTest({
        measures: [
          'visitors.countRollingThreeMonth'
        ],
        timeDimensions: [{
          dimension: 'visitors.created_at',
          granularity: 'month',
        }],
        order: [{
          id: 'visitors.created_at'
        }],
        timezone: 'America/Los_Angeles'
      }, [
        { visitors__created_at_month: '2016-09-01T00:00:00.000Z', visitors__count_rolling_three_month: '1' },
        { visitors__created_at_month: '2016-10-01T00:00:00.000Z', visitors__count_rolling_three_month: '1' },
        { visitors__created_at_month: '2016-11-01T00:00:00.000Z', visitors__count_rolling_three_month: '1' },
        { visitors__created_at_month: '2016-12-01T00:00:00.000Z', visitors__count_rolling_three_month: null },
        { visitors__created_at_month: '2017-01-01T00:00:00.000Z', visitors__count_rolling_three_month: '5' },
      ]);
    });
  } else {
    it.skip('rolling count without date range', () => {
      // Skipping because it works only in Tesseract
    });
  }

  if (getEnv('nativeSqlPlanner')) {
    it('rolling count proxy time dimension', async () => {
      await runQueryTest({
        measures: [
          'visitors.countRollingThreeMonth'
        ],
        dimensions: [
          'visitors.created_month'
        ],
        order: [{
          id: 'visitors.created_month'
        }],
        timezone: 'America/Los_Angeles'
      }, [
        { visitors__created_month: '2016-09-01T00:00:00.000Z', visitors__count_rolling_three_month: '1' },
        { visitors__created_month: '2016-10-01T00:00:00.000Z', visitors__count_rolling_three_month: '1' },
        { visitors__created_month: '2016-11-01T00:00:00.000Z', visitors__count_rolling_three_month: '1' },
        { visitors__created_month: '2016-12-01T00:00:00.000Z', visitors__count_rolling_three_month: null },
        { visitors__created_month: '2017-01-01T00:00:00.000Z', visitors__count_rolling_three_month: '5' },
      ]);
    });
  } else {
    it.skip('rolling count without date range', () => {
      // Skipping because it works only in Tesseract
    });
  }

  it('rolling qtd', async () => runQueryTest({
    measures: [
      'visitors.revenue_qtd'
    ],
    timeDimensions: [{
      dimension: 'visitors.created_at',
      granularity: 'day',
      dateRange: ['2017-01-05', '2017-01-10']
    }],
    order: [{
      id: 'visitors.created_at'
    }],
    timezone: 'America/Los_Angeles'
  }, [
    { visitors__created_at_day: '2017-01-05T00:00:00.000Z', visitors__revenue_qtd: '600' },
    { visitors__created_at_day: '2017-01-06T00:00:00.000Z', visitors__revenue_qtd: '1500' },
    { visitors__created_at_day: '2017-01-07T00:00:00.000Z', visitors__revenue_qtd: '1500' },
    { visitors__created_at_day: '2017-01-08T00:00:00.000Z', visitors__revenue_qtd: '1500' },
    { visitors__created_at_day: '2017-01-09T00:00:00.000Z', visitors__revenue_qtd: '1500' },
    { visitors__created_at_day: '2017-01-10T00:00:00.000Z', visitors__revenue_qtd: '1500' }
  ]));

  if (getEnv('nativeSqlPlanner')) {
    it('rolling qtd proxy', async () => runQueryTest({
      measures: [
        'visitors.revenue_qtd_proxy'
      ],
      timeDimensions: [{
        dimension: 'visitors.created_at',
        granularity: 'day',
        dateRange: ['2017-01-05', '2017-01-10']
      }],
      order: [{
        id: 'visitors.created_at'
      }],
      timezone: 'America/Los_Angeles'
    }, [
      { visitors__created_at_day: '2017-01-05T00:00:00.000Z', visitors__revenue_qtd_proxy: '600' },
      { visitors__created_at_day: '2017-01-06T00:00:00.000Z', visitors__revenue_qtd_proxy: '1500' },
      { visitors__created_at_day: '2017-01-07T00:00:00.000Z', visitors__revenue_qtd_proxy: '1500' },
      { visitors__created_at_day: '2017-01-08T00:00:00.000Z', visitors__revenue_qtd_proxy: '1500' },
      { visitors__created_at_day: '2017-01-09T00:00:00.000Z', visitors__revenue_qtd_proxy: '1500' },
      { visitors__created_at_day: '2017-01-10T00:00:00.000Z', visitors__revenue_qtd_proxy: '1500' }
    ]));
  } else {
    it.skip('rolling qtd proxy', () => {
      // Skipping because it works only in Tesseract
    });
  }

  it('CAGR', async () => runQueryTest({
    measures: [
      'visitors.revenue',
      'visitors.revenue_day_ago',
      'visitors.cagr_day'
    ],
    timeDimensions: [{
      dimension: 'visitors.created_at',
      granularity: 'day',
      dateRange: ['2016-12-01', '2017-01-31']
    }],
    order: [{
      id: 'visitors.created_at'
    }],
    timezone: 'America/Los_Angeles'
  },
  getEnv('nativeSqlPlanner') ?
    [
      {
        visitors__cagr_day: null,
        visitors__created_at_day: '2017-01-02T00:00:00.000Z',
        visitors__revenue: '100',
        visitors__revenue_day_ago: null,
      },
      {
        visitors__cagr_day: null,
        visitors__created_at_day: '2017-01-03T00:00:00.000Z',
        visitors__revenue: null,
        visitors__revenue_day_ago: '100',
      },
      {
        visitors__cagr_day: null,
        visitors__created_at_day: '2017-01-04T00:00:00.000Z',
        visitors__revenue: '200',
        visitors__revenue_day_ago: null,
      },
      { visitors__created_at_day: '2017-01-05T00:00:00.000Z', visitors__cagr_day: '150', visitors__revenue: '300', visitors__revenue_day_ago: '200' },
      { visitors__created_at_day: '2017-01-06T00:00:00.000Z', visitors__cagr_day: '300', visitors__revenue: '900', visitors__revenue_day_ago: '300' },

      {
        visitors__cagr_day: null,
        visitors__created_at_day: '2017-01-07T00:00:00.000Z',
        visitors__revenue: null,
        visitors__revenue_day_ago: '900',
      },

    ]

    : [
      { visitors__created_at_day: '2017-01-05T00:00:00.000Z', visitors__cagr_day: '150', visitors__revenue: '300', visitors__revenue_day_ago: '200' },
      { visitors__created_at_day: '2017-01-06T00:00:00.000Z', visitors__cagr_day: '300', visitors__revenue: '900', visitors__revenue_day_ago: '300' }
    ]));

  it('CAGR (no td in time_shift)', async () => runQueryTest({
    measures: [
      'visitors.revenue',
      'visitors.revenue_day_ago_no_td',
      'visitors.cagr_day'
    ],
    timeDimensions: [{
      dimension: 'visitors.created_at',
      granularity: 'day',
      dateRange: ['2016-12-01', '2017-01-31']
    }],
    order: [{
      id: 'visitors.created_at'
    }],
    timezone: 'America/Los_Angeles'
  },
  getEnv('nativeSqlPlanner') ?
    [
      {
        visitors__cagr_day: null,
        visitors__created_at_day: '2017-01-02T00:00:00.000Z',
        visitors__revenue: '100',
        visitors__revenue_day_ago_no_td: null,
      },
      {
        visitors__cagr_day: null,
        visitors__created_at_day: '2017-01-03T00:00:00.000Z',
        visitors__revenue: null,
        visitors__revenue_day_ago_no_td: '100',
      },
      {
        visitors__cagr_day: null,
        visitors__created_at_day: '2017-01-04T00:00:00.000Z',
        visitors__revenue: '200',
        visitors__revenue_day_ago_no_td: null,
      },
      { visitors__created_at_day: '2017-01-05T00:00:00.000Z', visitors__cagr_day: '150', visitors__revenue: '300', visitors__revenue_day_ago_no_td: '200' },
      { visitors__created_at_day: '2017-01-06T00:00:00.000Z', visitors__cagr_day: '300', visitors__revenue: '900', visitors__revenue_day_ago_no_td: '300' },

      {
        visitors__cagr_day: null,
        visitors__created_at_day: '2017-01-07T00:00:00.000Z',
        visitors__revenue: null,
        visitors__revenue_day_ago_no_td: '900',
      },

    ]

    : [
      { visitors__created_at_day: '2017-01-05T00:00:00.000Z', visitors__cagr_day: '150', visitors__revenue: '300', visitors__revenue_day_ago_no_td: '200' },
      { visitors__created_at_day: '2017-01-06T00:00:00.000Z', visitors__cagr_day: '300', visitors__revenue: '900', visitors__revenue_day_ago_no_td: '300' }
    ]));

  it('CAGR via view (td from main cube)', async () => runQueryTest({
    measures: [
      'visitors_multi_stage.revenue',
      'visitors_multi_stage.revenue_day_ago',
      'visitors_multi_stage.cagr_day'
    ],
    timeDimensions: [{
      dimension: 'visitors_multi_stage.created_at',
      granularity: 'day',
      dateRange: ['2016-12-01', '2017-01-31']
    }],
    order: [{
      id: 'visitors_multi_stage.created_at'
    }],
    timezone: 'America/Los_Angeles'
  },
  getEnv('nativeSqlPlanner') ?
    [
      {
        visitors_multi_stage__cagr_day: null,
        visitors_multi_stage__created_at_day: '2017-01-02T00:00:00.000Z',
        visitors_multi_stage__revenue: '100',
        visitors_multi_stage__revenue_day_ago: null,
      },
      {
        visitors_multi_stage__cagr_day: null,
        visitors_multi_stage__created_at_day: '2017-01-03T00:00:00.000Z',
        visitors_multi_stage__revenue: null,
        visitors_multi_stage__revenue_day_ago: '100',
      },
      {
        visitors_multi_stage__cagr_day: null,
        visitors_multi_stage__created_at_day: '2017-01-04T00:00:00.000Z',
        visitors_multi_stage__revenue: '200',
        visitors_multi_stage__revenue_day_ago: null,
      },
      { visitors_multi_stage__created_at_day: '2017-01-05T00:00:00.000Z', visitors_multi_stage__cagr_day: '150', visitors_multi_stage__revenue: '300', visitors_multi_stage__revenue_day_ago: '200' },
      { visitors_multi_stage__created_at_day: '2017-01-06T00:00:00.000Z', visitors_multi_stage__cagr_day: '300', visitors_multi_stage__revenue: '900', visitors_multi_stage__revenue_day_ago: '300' },

      {
        visitors_multi_stage__cagr_day: null,
        visitors_multi_stage__created_at_day: '2017-01-07T00:00:00.000Z',
        visitors_multi_stage__revenue: null,
        visitors_multi_stage__revenue_day_ago: '900',
      },
    ]
    : [
      { visitors_multi_stage__created_at_day: '2017-01-05T00:00:00.000Z', visitors_multi_stage__cagr_day: '150', visitors_multi_stage__revenue: '300', visitors_multi_stage__revenue_day_ago: '200' },
      { visitors_multi_stage__created_at_day: '2017-01-06T00:00:00.000Z', visitors_multi_stage__cagr_day: '300', visitors_multi_stage__revenue: '900', visitors_multi_stage__revenue_day_ago: '300' }
    ]));

  it('CAGR via view (td from joined cube)', async () => runQueryTest({
    measures: [
      'visitors_multi_stage.revenue',
      'visitors_multi_stage.revenue_day_ago_via_join',
    ],
    timeDimensions: [{
      dimension: 'visitors_multi_stage.create_date_created_at',
      granularity: 'day',
      dateRange: ['2016-12-01', '2017-01-31']
    }],
    order: [{
      id: 'visitors_multi_stage.create_date_created_at'
    }],
    timezone: 'America/Los_Angeles'
  },
  getEnv('nativeSqlPlanner') ?
    [
      {
        visitors_multi_stage__create_date_created_at_day: '2017-01-02T00:00:00.000Z',
        visitors_multi_stage__revenue: '100',
        visitors_multi_stage__revenue_day_ago_via_join: null,
      },
      {
        visitors_multi_stage__create_date_created_at_day: '2017-01-03T00:00:00.000Z',
        visitors_multi_stage__revenue: null,
        visitors_multi_stage__revenue_day_ago_via_join: '100',
      },
      {
        visitors_multi_stage__create_date_created_at_day: '2017-01-04T00:00:00.000Z',
        visitors_multi_stage__revenue: '200',
        visitors_multi_stage__revenue_day_ago_via_join: null,
      },
      { visitors_multi_stage__create_date_created_at_day: '2017-01-05T00:00:00.000Z', visitors_multi_stage__revenue: '300', visitors_multi_stage__revenue_day_ago_via_join: '200' },
      { visitors_multi_stage__create_date_created_at_day: '2017-01-06T00:00:00.000Z', visitors_multi_stage__revenue: '900', visitors_multi_stage__revenue_day_ago_via_join: '300' },

      {
        visitors_multi_stage__create_date_created_at_day: '2017-01-07T00:00:00.000Z',
        visitors_multi_stage__revenue: null,
        visitors_multi_stage__revenue_day_ago_via_join: '900',
      }
    ]
    : [
      { visitors_multi_stage__create_date_created_at_day: '2017-01-05T00:00:00.000Z', visitors_multi_stage__revenue: '300', visitors_multi_stage__revenue_day_ago_via_join: '200' },
      { visitors_multi_stage__create_date_created_at_day: '2017-01-06T00:00:00.000Z', visitors_multi_stage__revenue: '900', visitors_multi_stage__revenue_day_ago_via_join: '300' }
    ]));

  it('CAGR (no td in time_shift via view)', async () => runQueryTest({
    measures: [
      'visitors_multi_stage.revenue',
      'visitors_multi_stage.revenue_day_ago_no_td',
    ],
    timeDimensions: [{
      dimension: 'visitors_multi_stage.create_date_created_at',
      granularity: 'day',
      dateRange: ['2016-12-01', '2017-01-31']
    }],
    order: [{
      id: 'visitors_multi_stage.create_date_created_at'
    }],
    timezone: 'America/Los_Angeles'
  },
  getEnv('nativeSqlPlanner') ?
    [

      {
        visitors_multi_stage__create_date_created_at_day: '2017-01-02T00:00:00.000Z',
        visitors_multi_stage__revenue: '100',
        visitors_multi_stage__revenue_day_ago_no_td: null,
      },
      {
        visitors_multi_stage__create_date_created_at_day: '2017-01-03T00:00:00.000Z',
        visitors_multi_stage__revenue: null,
        visitors_multi_stage__revenue_day_ago_no_td: '100',
      },
      {
        visitors_multi_stage__create_date_created_at_day: '2017-01-04T00:00:00.000Z',
        visitors_multi_stage__revenue: '200',
        visitors_multi_stage__revenue_day_ago_no_td: null,
      },
      {
        visitors_multi_stage__create_date_created_at_day: '2017-01-05T00:00:00.000Z',
        visitors_multi_stage__revenue: '300',
        visitors_multi_stage__revenue_day_ago_no_td: '200',
      },
      {
        visitors_multi_stage__create_date_created_at_day: '2017-01-06T00:00:00.000Z',
        visitors_multi_stage__revenue: '900',
        visitors_multi_stage__revenue_day_ago_no_td: '300',
      },

      {
        visitors_multi_stage__create_date_created_at_day: '2017-01-07T00:00:00.000Z',
        visitors_multi_stage__revenue: null,
        visitors_multi_stage__revenue_day_ago_no_td: '900',
      },
    ]
    : [
      {
        visitors_multi_stage__create_date_created_at_day: '2017-01-05T00:00:00.000Z',
        visitors_multi_stage__revenue: '300',
        visitors_multi_stage__revenue_day_ago_no_td: '200',
      },
      {
        visitors_multi_stage__create_date_created_at_day: '2017-01-06T00:00:00.000Z',
        visitors_multi_stage__revenue: '900',
        visitors_multi_stage__revenue_day_ago_no_td: '300',
      }
    ]));

  it('sql utils', async () => runQueryTest({
    measures: [
      'visitors.visitor_count'
    ],
    timeDimensions: [{
      dimension: 'visitors.createdAtSqlUtils',
      granularity: 'day',
      dateRange: ['2017-01-01', '2017-01-10']
    }],
    order: [{
      id: 'visitors.createdAtSqlUtils'
    }],
    timezone: 'America/Los_Angeles'
  }, [
    { visitors__created_at_sql_utils_day: '2017-01-02T00:00:00.000Z', visitors__visitor_count: '1' },
    { visitors__created_at_sql_utils_day: '2017-01-04T00:00:00.000Z', visitors__visitor_count: '1' },
    { visitors__created_at_sql_utils_day: '2017-01-05T00:00:00.000Z', visitors__visitor_count: '1' },
    { visitors__created_at_sql_utils_day: '2017-01-06T00:00:00.000Z', visitors__visitor_count: '2' }
  ]));

  it('running total total', async () => runQueryTest({
    measures: [
      'visitors.revenueRunning'
    ],
    timeDimensions: [{
      dimension: 'visitors.created_at',
      dateRange: ['2017-01-01', '2017-01-10']
    }],
    order: [{
      id: 'visitors.created_at'
    }],
    timezone: 'America/Los_Angeles'
  }, [
    {
      visitors__revenue_running: '1500'
    }
  ]));

  it('running total ratio', async () => runQueryTest({
    measures: [
      'visitors.runningRevenuePerCount'
    ],
    timeDimensions: [{
      dimension: 'visitors.created_at',
      granularity: 'day',
      dateRange: ['2017-01-01', '2017-01-10']
    }],
    order: [{
      id: 'visitors.created_at'
    }],
    timezone: 'America/Los_Angeles'
  }, [
    { visitors__created_at_day: '2017-01-01T00:00:00.000Z', visitors__running_revenue_per_count: null },
    { visitors__created_at_day: '2017-01-02T00:00:00.000Z', visitors__running_revenue_per_count: '100' },
    { visitors__created_at_day: '2017-01-03T00:00:00.000Z', visitors__running_revenue_per_count: '100' },
    { visitors__created_at_day: '2017-01-04T00:00:00.000Z', visitors__running_revenue_per_count: '150' },
    { visitors__created_at_day: '2017-01-05T00:00:00.000Z', visitors__running_revenue_per_count: '200' },
    { visitors__created_at_day: '2017-01-06T00:00:00.000Z', visitors__running_revenue_per_count: '300' },
    { visitors__created_at_day: '2017-01-07T00:00:00.000Z', visitors__running_revenue_per_count: '300' },
    { visitors__created_at_day: '2017-01-08T00:00:00.000Z', visitors__running_revenue_per_count: '300' },
    { visitors__created_at_day: '2017-01-09T00:00:00.000Z', visitors__running_revenue_per_count: '300' },
    { visitors__created_at_day: '2017-01-10T00:00:00.000Z', visitors__running_revenue_per_count: '300' }
  ]));

  it('hll rolling (BigQuery)', async () => {
    await compiler.compile();

    const query = new BigqueryQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors.countDistinctApproxRolling'
      ],
      timeDimensions: [{
        dimension: 'visitors.created_at',
        granularity: 'day',
        dateRange: ['2017-01-01', '2017-01-10']
      }],
      order: [{
        id: 'visitors.created_at'
      }],
      timezone: 'America/Los_Angeles'
    });

    expect(query.buildSqlAndParams()[0]).toMatch(/HLL_COUNT\.MERGE/);
    expect(query.buildSqlAndParams()[0]).toMatch(/HLL_COUNT\.INIT/);
  });

  it('offset (PrestoQuery), refs #988', async () => {
    await compiler.compile();

    const query = new PrestodbQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors.visitor_revenue',
      ],
      timeDimensions: [{
        dimension: 'visitors.created_at',
      }],
      order: [{
        id: 'visitors.created_at'
      }],
      timezone: 'America/Los_Angeles',
      offset: 5,
      rowLimit: 5,
    });

    console.log(query.buildSqlAndParams());

    expect(query.buildSqlAndParams()[0]).toMatch(/OFFSET (\d)\s+LIMIT (\d)/);
  });

  it('calculated join', async () => {
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitor_checkins.revenue_per_checkin'
      ],
      timeDimensions: [],
      timezone: 'America/Los_Angeles'
    });

    console.log(query.buildSqlAndParams());

    return dbRunner.testQuery(query.buildSqlAndParams()).then(res => {
      console.log(JSON.stringify(res));
      expect(res).toEqual(
        [{ vc__revenue_per_checkin: '50' }]
      );
    });
  });

  it('filter join', async () => {
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitor_checkins.google_sourced_checkins'
      ],
      timeDimensions: [],
      timezone: 'America/Los_Angeles'
    });

    console.log(query.buildSqlAndParams());

    return dbRunner.testQuery(query.buildSqlAndParams()).then(res => {
      console.log(JSON.stringify(res));
      expect(res).toEqual(
        [{ vc__google_sourced_checkins: '1' }]
      );
    });
  });

  it('filter join not multiplied', async () => {
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitor_checkins.google_sourced_checkins'
      ],
      timeDimensions: [],
      filters: [
        { dimension: 'cards.id', operator: 'equals', values: ['3'] }
      ],
      timezone: 'America/Los_Angeles'
    });

    console.log(query.buildSqlAndParams());

    return dbRunner.testQuery(query.buildSqlAndParams()).then(res => {
      console.log(JSON.stringify(res));
      expect(res).toEqual(
        [{ vc__google_sourced_checkins: '1' }]
      );
    });
  });

  it('having filter', async () => {
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors.visitor_count'
      ],
      dimensions: [
        'visitors.source'
      ],
      timeDimensions: [],
      timezone: 'America/Los_Angeles',
      filters: [{
        dimension: 'visitors.visitor_count',
        operator: 'gt',
        values: ['1']
      }],
      order: [{
        id: 'visitors.source'
      }]
    });

    console.log(query.buildSqlAndParams());

    return dbRunner.testQuery(query.buildSqlAndParams()).then(res => {
      console.log(JSON.stringify(res));
      expect(res).toEqual(
        [{
          visitors__source: 'some',
          visitors__visitor_count: '2'
        }, {
          visitors__source: null,
          visitors__visitor_count: '3'
        }]
      );
    });
  });

  it('having filter (time measure)', async () => {
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors.min_created_at'
      ],
      dimensions: [
        'visitors.source'
      ],
      timeDimensions: [],
      timezone: 'America/Los_Angeles',
      filters: [{
        dimension: 'visitors.min_created_at',
        operator: 'inDateRange',
        values: ['2017-01-01', '2018-01-01']
      }],
      order: [{
        id: 'visitors.source'
      }]
    });

    console.log(query.buildSqlAndParams());

    return dbRunner.testQuery(query.buildSqlAndParams()).then(res => {
      console.log(JSON.stringify(res));
      expect(res).toEqual([
        {
          visitors__min_created_at: '2017-01-06T00:00:00.000Z',
          visitors__source: 'google',
        },
        {
          visitors__min_created_at: '2017-01-03T00:00:00.000Z',
          visitors__source: 'some',
        },
      ]);
    });
  });

  it('having filter without measure', async () => {
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [],
      dimensions: [
        'visitors.source'
      ],
      timeDimensions: [],
      timezone: 'America/Los_Angeles',
      filters: [{
        dimension: 'visitors.visitor_count',
        operator: 'gt',
        values: ['1']
      }],
      order: [{
        id: 'visitors.source'
      }]
    });

    console.log(query.buildSqlAndParams());

    return dbRunner.testQuery(query.buildSqlAndParams()).then(res => {
      console.log(JSON.stringify(res));
      expect(res).toEqual(
        [{
          visitors__source: 'some'
        }, {
          visitors__source: null
        }]
      );
    });
  });

  it('having filter without measure with join', async () => {
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [],
      dimensions: [
        'visitors.source'
      ],
      timeDimensions: [],
      timezone: 'America/Los_Angeles',
      filters: [{
        dimension: 'visitor_checkins.revenue_per_checkin',
        operator: 'gte',
        values: ['60']
      }],
      order: [{
        id: 'visitors.source'
      }]
    });

    console.log(query.buildSqlAndParams());

    return dbRunner.testQuery(query.buildSqlAndParams()).then(res => {
      console.log(JSON.stringify(res));
      expect(res).toEqual(
        [{
          visitors__source: 'some'
        }]
      );
    });
  });

  it('having filter without measure single multiplied', async () => {
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [],
      dimensions: [
        'visitors.source'
      ],
      timeDimensions: [],
      timezone: 'America/Los_Angeles',
      filters: [{
        dimension: 'visitors.visitor_revenue',
        operator: 'gte',
        values: ['1']
      }, {
        dimension: 'visitor_checkins.source',
        operator: 'equals',
        values: ['google']
      }],
      order: [{
        id: 'visitors.source'
      }]
    });

    console.log(query.buildSqlAndParams());

    return dbRunner.testQuery(query.buildSqlAndParams()).then(res => {
      console.log(JSON.stringify(res));
      expect(res).toEqual(
        [{
          visitors__source: 'some'
        }]
      );
    });
  });

  it('subquery', async () => {
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors.visitor_count'
      ],
      dimensions: [
        'visitors.checkins'
      ],
      timeDimensions: [{
        dimension: 'visitors.created_at',
        granularity: 'day',
        dateRange: ['2017-01-01', '2017-01-30']
      }],
      timezone: 'America/Los_Angeles',
      filters: [],
      order: [{
        id: 'visitors.checkins'
      }]
    });

    console.log(query.buildSqlAndParams());

    return dbRunner.testQuery(query.buildSqlAndParams()).then(res => {
      console.log(JSON.stringify(res));
      expect(res).toEqual(
        [{
          visitors__checkins: '0',
          visitors__created_at_day: '2017-01-06T00:00:00.000Z',
          visitors__visitor_count: '2'
        }, {
          visitors__checkins: '1',
          visitors__created_at_day: '2017-01-05T00:00:00.000Z',
          visitors__visitor_count: '1'
        }, {
          visitors__checkins: '2',
          visitors__created_at_day: '2017-01-04T00:00:00.000Z',
          visitors__visitor_count: '1'
        }, {
          visitors__checkins: '3',
          visitors__created_at_day: '2017-01-02T00:00:00.000Z',
          visitors__visitor_count: '1'
        }]
      );
    });
  });

  it('subquery rolling', async () => {
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors.visitor_count'
      ],
      dimensions: [
        'visitors.checkinsRolling'
      ],
      timeDimensions: [{
        dimension: 'visitors.created_at',
        granularity: 'day',
        dateRange: ['2017-01-01', '2017-01-30']
      }],
      timezone: 'America/Los_Angeles',
      filters: [],
      order: [{
        id: 'visitors.checkins'
      }]
    });

    console.log(query.buildSqlAndParams());

    return dbRunner.testQuery(query.buildSqlAndParams()).then(res => {
      console.log(JSON.stringify(res));
      expect(res).toEqual(
        [{
          visitors__checkins_rolling: '0',
          visitors__created_at_day: '2017-01-06T00:00:00.000Z',
          visitors__visitor_count: '2'
        }, {
          visitors__checkins_rolling: '1',
          visitors__created_at_day: '2017-01-05T00:00:00.000Z',
          visitors__visitor_count: '1'
        }, {
          visitors__checkins_rolling: '2',
          visitors__created_at_day: '2017-01-04T00:00:00.000Z',
          visitors__visitor_count: '1'
        }, {
          visitors__checkins_rolling: '3',
          visitors__created_at_day: '2017-01-02T00:00:00.000Z',
          visitors__visitor_count: '1'
        }]
      );
    });
  });

  it('subquery with propagated filters', async () => {
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors.visitor_count'
      ],
      dimensions: [
        'visitors.checkinsWithPropagation'
      ],
      timeDimensions: [{
        dimension: 'visitors.created_at',
        granularity: 'day',
        dateRange: ['2017-01-01', '2017-01-30']
      }],
      timezone: 'America/Los_Angeles',
      filters: [],
      order: [{
        id: 'visitors.checkins'
      }]
    });

    console.log(query.buildSqlAndParams());

    return dbRunner.testQuery(query.buildSqlAndParams()).then(res => {
      console.log(JSON.stringify(res));
      expect(res).toEqual(
        [{
          visitors__checkins_with_propagation: '0',
          visitors__created_at_day: '2017-01-06T00:00:00.000Z',
          visitors__visitor_count: '2'
        }, {
          visitors__checkins_with_propagation: '1',
          visitors__created_at_day: '2017-01-05T00:00:00.000Z',
          visitors__visitor_count: '1'
        }, {
          visitors__checkins_with_propagation: '2',
          visitors__created_at_day: '2017-01-04T00:00:00.000Z',
          visitors__visitor_count: '1'
        }, {
          visitors__checkins_with_propagation: '3',
          visitors__created_at_day: '2017-01-02T00:00:00.000Z',
          visitors__visitor_count: '1'
        }]
      );
    });
  });

  it('average subquery', async () => {
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors.averageCheckins'
      ],
      timeDimensions: [{
        dimension: 'visitors.created_at',
        granularity: 'day',
        dateRange: ['2017-01-01', '2017-01-30']
      }],
      timezone: 'America/Los_Angeles',
      filters: [{
        dimension: 'visitor_checkins.source',
        operator: 'equals',
        values: ['google']
      }],
      order: [{
        id: 'visitors.averageCheckins'
      }]
    });

    console.log(query.buildSqlAndParams());

    return dbRunner.testQuery(query.buildSqlAndParams()).then(res => {
      console.log(JSON.stringify(res));
      expect(res).toEqual(
        [{ visitors__created_at_day: '2017-01-02T00:00:00.000Z', visitors__average_checkins: '6.0000000000000000' }]
      );
    });
  });

  it('subquery without measure', () => runQueryTest({
    dimensions: [
      'visitors.subQueryFail'
    ],
    timeDimensions: [{
      dimension: 'visitors.created_at',
      granularity: 'day',
      dateRange: ['2017-01-01', '2017-01-30']
    }],
    timezone: 'America/Los_Angeles',
    order: [{
      id: 'visitors.created_at'
    }]
  }, [
    {
      visitors__min_visitor_checkin_date_day: '2017-01-02T00:00:00.000Z',
      visitors__visitor_count: '1'
    },
    {
      visitors__min_visitor_checkin_date_day: '2017-01-04T00:00:00.000Z',
      visitors__visitor_count: '1'
    },
    {
      visitors__min_visitor_checkin_date_day: '2017-01-05T00:00:00.000Z',
      visitors__visitor_count: '1'
    }
  ]).then(() => {
    throw new Error();
  }).catch((error) => {
    console.log('Error: ', error);
    expect(error).toBeInstanceOf(UserError);
  }));

  it('min date subquery', () => runQueryTest({
    measures: [
      'visitors.visitor_count'
    ],
    timeDimensions: [{
      dimension: 'visitors.minVisitorCheckinDate',
      granularity: 'day',
      dateRange: ['2017-01-01', '2017-01-30']
    }],
    timezone: 'America/Los_Angeles',
    order: [{
      id: 'visitors.minVisitorCheckinDate'
    }]
  }, [
    {
      visitors__min_visitor_checkin_date_day: '2017-01-02T00:00:00.000Z',
      visitors__visitor_count: '1'
    },
    {
      visitors__min_visitor_checkin_date_day: '2017-01-04T00:00:00.000Z',
      visitors__visitor_count: '1'
    },
    {
      visitors__min_visitor_checkin_date_day: '2017-01-05T00:00:00.000Z',
      visitors__visitor_count: '1'
    }
  ]));

  it('min date subquery with error', () => runQueryTest({
    measures: [
      'visitors.visitor_count'
    ],
    timeDimensions: [{
      dimension: 'visitors.minVisitorCheckinDate1',
      granularity: 'day',
      dateRange: ['2017-01-01', '2017-01-30']
    }],
    timezone: 'America/Los_Angeles',
    order: [{
      id: 'visitors.minVisitorCheckinDate1'
    }]
  }, []).catch((error) => {
    expect(error).toBeInstanceOf(UserError);
  }));

  it('subquery dimension with join', () => runQueryTest({
    measures: [
      'visitors.visitor_revenue'
    ],
    dimensions: ['visitor_checkins.cardsCount'],
    timezone: 'America/Los_Angeles',
    order: [{
      id: 'visitor_checkins.cardsCount'
    }]
  }, [
    {
      vc__cards_count: '0',
      visitors__visitor_revenue: '300'
    },
    {
      vc__cards_count: '1',
      visitors__visitor_revenue: '100'
    },
    {
      vc__cards_count: null,
      visitors__visitor_revenue: null
    }
  ]));

  it('ungrouped cumulative query', async () => {
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitor_checkins.visitor_checkins_count',
        'visitor_checkins.visitorCheckinsRolling',
      ],
      dimensions: [
        'visitor_checkins.id'
      ],
      timeDimensions: [{
        dimension: 'visitor_checkins.created_at',
        granularity: 'day',
        dateRange: ['2017-01-01', '2017-01-06']
      }],
      timezone: 'America/Los_Angeles',
      filters: [],
      order: [{
        id: 'visitor_checkins.id'
      },
      {
        id: 'visitor_checkins.created_at'
      }
      ],
      ungrouped: true
    });

    console.log(query.buildSqlAndParams());

    return dbRunner.testQuery(query.buildSqlAndParams()).then(res => {
      console.log(JSON.stringify(res));

      const expected = getEnv('nativeSqlPlanner') ?
        [
          {
            vc__id: 3,
            vc__created_at_day: '2017-01-04T00:00:00.000Z',
            vc__visitor_checkins_count: 1,
            vc__visitor_checkins_rolling: 1
          },
          {
            vc__created_at_day: '2017-01-05T00:00:00.000Z',
            vc__id: 3,
            vc__visitor_checkins_count: null,
            vc__visitor_checkins_rolling: 1,
          },
          {
            vc__created_at_day: '2017-01-06T00:00:00.000Z',
            vc__id: 3,
            vc__visitor_checkins_count: null,
            vc__visitor_checkins_rolling: 1,
          },
          {
            vc__id: 4,
            vc__created_at_day: '2017-01-04T00:00:00.000Z',
            vc__visitor_checkins_count: 1,
            vc__visitor_checkins_rolling: 1

          },
          {
            vc__created_at_day: '2017-01-05T00:00:00.000Z',
            vc__id: 4,
            vc__visitor_checkins_count: null,
            vc__visitor_checkins_rolling: 1,
          },
          {
            vc__created_at_day: '2017-01-06T00:00:00.000Z',
            vc__id: 4,
            vc__visitor_checkins_count: null,
            vc__visitor_checkins_rolling: 1,
          },
          {
            vc__id: 5,
            vc__created_at_day: '2017-01-04T00:00:00.000Z',
            vc__visitor_checkins_count: 1,
            vc__visitor_checkins_rolling: 1
          },
          {
            vc__created_at_day: '2017-01-05T00:00:00.000Z',
            vc__id: 5,
            vc__visitor_checkins_count: null,
            vc__visitor_checkins_rolling: 1,
          },
          {
            vc__created_at_day: '2017-01-06T00:00:00.000Z',
            vc__id: 5,
            vc__visitor_checkins_count: null,
            vc__visitor_checkins_rolling: 1,
          },
          {
            vc__id: 6,
            vc__created_at_day: '2017-01-05T00:00:00.000Z',
            vc__visitor_checkins_count: 1,
            vc__visitor_checkins_rolling: 1
          },

          {
            vc__created_at_day: '2017-01-06T00:00:00.000Z',
            vc__id: 6,
            vc__visitor_checkins_count: null,
            vc__visitor_checkins_rolling: 1,
          },
          {
            vc__created_at_day: '2017-01-01T00:00:00.000Z',
            vc__id: null,
            vc__visitor_checkins_count: null,
            vc__visitor_checkins_rolling: null,
          },
          {
            vc__created_at_day: '2017-01-02T00:00:00.000Z',
            vc__id: null,
            vc__visitor_checkins_count: null,
            vc__visitor_checkins_rolling: null,
          },
          {
            vc__created_at_day: '2017-01-03T00:00:00.000Z',
            vc__id: null,
            vc__visitor_checkins_count: null,
            vc__visitor_checkins_rolling: null,
          },
        ]
        : [
          {
            vc__id: 3,
            vc__created_at_day: '2017-01-04T00:00:00.000Z',
            vc__visitor_checkins_count: 1,
            vc__visitor_checkins_rolling: 1
          },
          {
            vc__id: 4,
            vc__created_at_day: '2017-01-04T00:00:00.000Z',
            vc__visitor_checkins_count: 1,
            vc__visitor_checkins_rolling: 1
          },
          {
            vc__id: 5,
            vc__created_at_day: '2017-01-04T00:00:00.000Z',
            vc__visitor_checkins_count: 1,
            vc__visitor_checkins_rolling: 1
          },
          {
            vc__id: 6,
            vc__created_at_day: '2017-01-05T00:00:00.000Z',
            vc__visitor_checkins_count: 1,
            vc__visitor_checkins_rolling: 1
          }
        ];
      expect(res).toEqual(
        expected
      );
    });
  });

  /// Test that query with segment member expression, that references dimension, that is covered by pre-agg
  /// would _not_ trigger stuff like `path.split is not a function` due to unexpected member expression
  it('pre-aggregation with segment member expression', async () => {
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      timeDimensions: [{
        dimension: 'visitors.created_at',
        granularity: 'day'
      }],
      segments: [
        {
          // eslint-disable-next-line no-new-func
          expression: new Function(
            'visitor_checkins',
            // eslint-disable-next-line no-template-curly-in-string
            'return `${visitor_checkins.source} IS NOT NULL`'
          ),
          expressionName: 'source_is_some',
          // eslint-disable-next-line no-template-curly-in-string
          definition: '${visitor_checkins.source} IS NOT NULL',
          cubeName: 'visitor_checkins',
        },
      ],
      timezone: 'America/Los_Angeles',
      order: [],
      preAggregationsSchema: ''
    });

    const preAggregationsDescription: any = query.preAggregations?.preAggregationsDescription()[0];

    const res = await testWithPreAggregation(preAggregationsDescription, query);
    expect(res).toEqual(
      [{
        visitors__created_at_day: '2017-01-02T00:00:00.000Z',
      }]
    );
  });

  it('join rollup pre-aggregation', async () => {
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors.per_visitor_revenue'
      ],
      dimensions: ['visitor_checkins.source'],
      timeDimensions: [{
        dimension: 'visitors.created_at',
        granularity: 'day',
        dateRange: ['2017-01-01', '2017-01-30']
      }],
      timezone: 'America/Los_Angeles',
      order: [{
        id: 'visitors.created_at'
      }],
      filters: [{
        dimension: 'visitors.per_visitor_revenue',
        operator: 'gt',
        values: ['50']
      }, {
        dimension: 'visitor_checkins.source',
        operator: 'equals',
        values: ['google']
      }],
      preAggregationsSchema: ''
    });

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);
    const preAggregationsDescription: any = query.preAggregations?.preAggregationsDescription()[0];
    console.log(preAggregationsDescription);

    const res = await testWithPreAggregation(preAggregationsDescription, query);
    console.log(JSON.stringify(res));
    expect(res).toEqual(
      [
        {
          vc__source: 'google',
          visitors__created_at_day: '2017-01-02T00:00:00.000Z',
          visitors__per_visitor_revenue: '100'
        }
      ]
    );
  });

  it('join rollup total pre-aggregation', async () => {
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors.visitor_revenue'
      ],
      timeDimensions: [{
        dimension: 'visitors.created_at',
        granularity: 'day',
        dateRange: ['2017-01-01', '2017-01-30']
      }],
      dimensions: ['visitor_checkins.source'],
      timezone: 'America/Los_Angeles',
      order: [],
      filters: [{
        dimension: 'visitor_checkins.source',
        operator: 'equals',
        values: ['google']
      }],
      preAggregationsSchema: ''
    });

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);
    const preAggregationsDescription: any = query.preAggregations?.preAggregationsDescription()[0];
    console.log(preAggregationsDescription);

    const res = await testWithPreAggregation(preAggregationsDescription, query);
    console.log(JSON.stringify(res));
    expect(res).toEqual(
      [{
        vc__source: 'google',
        visitors__created_at_day: '2017-01-02T00:00:00.000Z',
        visitors__visitor_revenue: '100'
      }]
    );
  });

  it('security context', async () => {
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitor_checkins.revenue_per_checkin'
      ],
      timeDimensions: [],
      timezone: 'America/Los_Angeles',
      contextSymbols: {
        securityContext: { source: 'some' }
      }
    });

    console.log(query.buildSqlAndParams());

    return dbRunner.testQuery(query.buildSqlAndParams()).then(res => {
      console.log(JSON.stringify(res));
      expect(res).toEqual(
        [{ vc__revenue_per_checkin: '60' }]
      );
    });
  });

  it('security context array', async () => {
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitor_checkins.revenue_per_checkin'
      ],
      timeDimensions: [],
      timezone: 'America/Los_Angeles',
      contextSymbols: {
        securityContext: {
          sourceArray: ['some', 'google']
        }
      }
    });

    console.log(query.buildSqlAndParams());

    return dbRunner.testQuery(query.buildSqlAndParams()).then(res => {
      console.log(JSON.stringify(res));
      expect(res).toEqual(
        [{ vc__revenue_per_checkin: '50' }]
      );
    });
  });

  it('reference cube sql', () => runQueryTest({
    measures: [
      'ReferenceVisitors.count'
    ],
    timezone: 'America/Los_Angeles',
    order: [],
    timeDimensions: [{
      dimension: 'ReferenceVisitors.createdAt',
      dateRange: ['2017-01-01', '2017-01-30']
    }],
  }, [{ reference_visitors__count: '1' }]));

  it('Filtered count without primaryKey', () => runQueryTest({
    measures: [
      'ReferenceVisitors.googleSourcedCount'
    ],
    timezone: 'America/Los_Angeles',
    order: [],
    timeDimensions: [{
      dimension: 'ReferenceVisitors.createdAt',
      dateRange: ['2016-12-01', '2017-03-30']
    }],
  }, [{ reference_visitors__google_sourced_count: '1' }]));

  it('ungrouped filtered count', () => runQueryTest({
    measures: [
      'visitor_checkins.google_sourced_checkins',
    ],
    timezone: 'America/Los_Angeles',
    order: [{
      id: 'visitor_checkins.created_at',
    }],
    timeDimensions: [{
      dimension: 'visitor_checkins.created_at',
      granularity: 'day',
      dateRange: ['2016-12-01', '2017-03-30'],
    }],
    ungrouped: true,
    allowUngroupedWithoutPrimaryKey: true,
  }, [
    { vc__created_at_day: '2017-01-02T00:00:00.000Z', vc__google_sourced_checkins: null },
    { vc__created_at_day: '2017-01-03T00:00:00.000Z', vc__google_sourced_checkins: null },
    { vc__created_at_day: '2017-01-04T00:00:00.000Z', vc__google_sourced_checkins: null },
    { vc__created_at_day: '2017-01-04T00:00:00.000Z', vc__google_sourced_checkins: null },
    { vc__created_at_day: '2017-01-04T00:00:00.000Z', vc__google_sourced_checkins: null },
    { vc__created_at_day: '2017-01-05T00:00:00.000Z', vc__google_sourced_checkins: 1 },
  ]));

  it('ungrouped filtered distinct count', () => runQueryTest({
    measures: [
      'visitor_checkins.unique_google_sourced_checkins',
    ],
    timezone: 'America/Los_Angeles',
    order: [{
      id: 'visitor_checkins.created_at',
    }],
    timeDimensions: [{
      dimension: 'visitor_checkins.created_at',
      granularity: 'day',
      dateRange: ['2016-12-01', '2017-03-30'],
    }],
    ungrouped: true,
    allowUngroupedWithoutPrimaryKey: true,
  }, [
    { vc__created_at_day: '2017-01-02T00:00:00.000Z', vc__unique_google_sourced_checkins: null },
    { vc__created_at_day: '2017-01-03T00:00:00.000Z', vc__unique_google_sourced_checkins: null },
    { vc__created_at_day: '2017-01-04T00:00:00.000Z', vc__unique_google_sourced_checkins: null },
    { vc__created_at_day: '2017-01-04T00:00:00.000Z', vc__unique_google_sourced_checkins: null },
    { vc__created_at_day: '2017-01-04T00:00:00.000Z', vc__unique_google_sourced_checkins: null },
    { vc__created_at_day: '2017-01-05T00:00:00.000Z', vc__unique_google_sourced_checkins: 1 },
  ]));

  it('ungrouped ratio measure', () => runQueryTest({
    measures: [
      'visitor_checkins.unique_sources_per_checking',
    ],
    timezone: 'America/Los_Angeles',
    order: [{
      id: 'visitor_checkins.created_at',
    }],
    timeDimensions: [{
      dimension: 'visitor_checkins.created_at',
      granularity: 'day',
      dateRange: ['2016-12-01', '2017-03-30'],
    }],
    ungrouped: true,
    allowUngroupedWithoutPrimaryKey: true,
  }, [
    { vc__created_at_day: '2017-01-02T00:00:00.000Z', vc__unique_sources_per_checking: 1 },
    { vc__created_at_day: '2017-01-03T00:00:00.000Z', vc__unique_sources_per_checking: 1 },
    { vc__created_at_day: '2017-01-04T00:00:00.000Z', vc__unique_sources_per_checking: 1 },
    { vc__created_at_day: '2017-01-04T00:00:00.000Z', vc__unique_sources_per_checking: 1 },
    { vc__created_at_day: '2017-01-04T00:00:00.000Z', vc__unique_sources_per_checking: 1 },
    { vc__created_at_day: '2017-01-05T00:00:00.000Z', vc__unique_sources_per_checking: 1 },
  ]));

  it('builds geo dimension', () => runQueryTest({
    dimensions: [
      'visitors.location'
    ],
    timezone: 'America/Los_Angeles',
    order: [{ id: 'visitors.location' }],
  }, [
    { visitors__location: '120.120,10.60' },
    { visitors__location: '120.120,40.60' },
    { visitors__location: '120.120,58.10' },
    { visitors__location: '120.120,58.60' },
    { visitors__location: '120.120,70.60' }
  ]));

  it('applies measure_filter type filter', () => runQueryTest({
    dimensions: [
      'visitors.id'
    ],
    filters: [{
      dimension: 'visitors.visitor_revenue',
      operator: 'measure_filter'
    }],
    timezone: 'America/Los_Angeles',
    order: [{ id: 'visitors.location' }],
  }, [
    { visitors__id: 1 },
    { visitors__id: 2 }
  ]));

  it(
    'contains filter',
    () => runQueryTest({
      measures: [],
      dimensions: [
        'visitors.source'
      ],
      timeDimensions: [],
      timezone: 'America/Los_Angeles',
      filters: [{
        dimension: 'visitor_checkins.source',
        operator: 'contains',
        values: ['goo']
      }],
      order: [{
        id: 'visitors.source'
      }]
    }, [
      { visitors__source: 'some' }
    ])
  );

  it(
    'contains multiple value filter',
    () => runQueryTest({
      measures: [],
      dimensions: [
        'visitor_checkins_sources.source'
      ],
      timeDimensions: [],
      timezone: 'America/Los_Angeles',
      filters: [{
        dimension: 'visitor_checkins_sources.source',
        operator: 'contains',
        values: ['goo']
      }, {
        dimension: 'visitor_checkins_sources.source',
        operator: 'contains',
        values: ['gle']
      }],
      order: [{
        id: 'visitor_checkins_sources.source'
      }]
    }, [
      { visitor_checkins_sources__source: 'google' }
    ])
  );

  it(
    'contains null filter',
    () => runQueryTest({
      measures: [],
      dimensions: [
        'visitors.source'
      ],
      timeDimensions: [],
      timezone: 'America/Los_Angeles',
      filters: [{
        dimension: 'visitors.source',
        operator: 'contains',
        values: ['goo', null]
      }],
      order: [{
        id: 'visitors.source'
      }]
    }, [
      { visitors__source: 'google' },
      { visitors__source: null }
    ])
  );

  it(
    'null filter',
    () => runQueryTest({
      measures: [],
      dimensions: [
        'visitors.source'
      ],
      timeDimensions: [],
      timezone: 'America/Los_Angeles',
      filters: [{
        dimension: 'visitors.source',
        operator: 'equals',
        values: ['google', null]
      }],
      order: [{
        id: 'visitors.source'
      }]
    }, [
      { visitors__source: 'google' },
      { visitors__source: null },
    ])
  );

  it(
    'not equals filter',
    () => runQueryTest({
      measures: [],
      dimensions: [
        'visitors.source'
      ],
      timeDimensions: [],
      timezone: 'America/Los_Angeles',
      filters: [{
        dimension: 'visitors.source',
        operator: 'notEquals',
        values: ['google']
      }],
      order: [{
        id: 'visitors.source'
      }]
    }, [
      { visitors__source: 'some' },
      { visitors__source: null },
    ])
  );

  it(
    'equals NULL filter',
    () => runQueryTest({
      measures: [
        'visitor_checkins_sources.count'
      ],
      dimensions: [
        'visitor_checkins_sources.visitor_id'
      ],
      timeDimensions: [],
      timezone: 'America/Los_Angeles',
      filters: [{
        dimension: 'visitor_checkins_sources.source',
        operator: 'equals',
        values: [null]
      }],
      order: [{
        id: 'visitor_checkins_sources.visitor_id'
      }]
    }, [
      {
        visitor_checkins_sources__visitor_id: 1,
        visitor_checkins_sources__count: '2'
      },
      {
        visitor_checkins_sources__visitor_id: 2,
        visitor_checkins_sources__count: '2'
      },
      {
        visitor_checkins_sources__visitor_id: 3,
        visitor_checkins_sources__count: '1'
      }
    ])
  );

  it(
    'notSet(IS NULL) filter',
    () => runQueryTest({
      measures: [
        'visitor_checkins_sources.count'
      ],
      dimensions: [
        'visitor_checkins_sources.visitor_id'
      ],
      timeDimensions: [],
      timezone: 'America/Los_Angeles',
      filters: [{
        dimension: 'visitor_checkins_sources.source',
        operator: 'notSet',
      }],
      order: [{
        id: 'visitor_checkins_sources.visitor_id'
      }]
    }, [
      {
        visitor_checkins_sources__visitor_id: 1,
        visitor_checkins_sources__count: '2'
      },
      {
        visitor_checkins_sources__visitor_id: 2,
        visitor_checkins_sources__count: '2'
      },
      {
        visitor_checkins_sources__visitor_id: 3,
        visitor_checkins_sources__count: '1'
      }
    ])
  );

  it(
    'notEquals NULL filter',
    () => runQueryTest({
      measures: [
        'visitor_checkins_sources.count'
      ],
      dimensions: [
        'visitor_checkins_sources.visitor_id'
      ],
      timeDimensions: [],
      timezone: 'America/Los_Angeles',
      filters: [{
        dimension: 'visitor_checkins_sources.source',
        operator: 'notEquals',
        values: [null]
      }],
      order: [{
        id: 'visitor_checkins_sources.visitor_id'
      }]
    }, [
      {
        visitor_checkins_sources__visitor_id: 1,
        visitor_checkins_sources__count: '1'
      }
    ])
  );

  it(
    'set(IS NOT NULL) filter',
    () => runQueryTest({
      measures: [
        'visitor_checkins_sources.count'
      ],
      dimensions: [
        'visitor_checkins_sources.visitor_id'
      ],
      timeDimensions: [],
      timezone: 'America/Los_Angeles',
      filters: [{
        dimension: 'visitor_checkins_sources.source',
        operator: 'set',
      }],
      order: [{
        id: 'visitor_checkins_sources.visitor_id'
      }]
    }, [
      {
        visitor_checkins_sources__visitor_id: 1,
        visitor_checkins_sources__count: '1'
      }
    ])
  );

  it(
    'source is notSet(IS NULL) "or" source is google filter',
    () => runQueryTest({
      measures: [
        'visitor_checkins_sources.count'
      ],
      dimensions: [
        'visitor_checkins_sources.visitor_id'
      ],
      timeDimensions: [],
      timezone: 'America/Los_Angeles',
      filters: [{
        or: [
          {
            dimension: 'visitor_checkins_sources.source',
            operator: 'notSet',
          },
          {
            dimension: 'visitor_checkins_sources.source',
            operator: 'equals',
            values: ['google']
          }
        ]
      }],
      order: [{
        id: 'visitor_checkins_sources.visitor_id'
      }]
    }, [
      {
        visitor_checkins_sources__visitor_id: 1,
        visitor_checkins_sources__count: '3'
      },
      {
        visitor_checkins_sources__visitor_id: 2,
        visitor_checkins_sources__count: '2'
      },
      {
        visitor_checkins_sources__visitor_id: 3,
        visitor_checkins_sources__count: '1'
      }
    ])
  );

  it('year granularity', () => runQueryTest({
    measures: [
      'visitors.visitor_count'
    ],
    timeDimensions: [{
      dimension: 'visitors.created_at',
      granularity: 'year',
      dateRange: ['2016-01-09', '2017-01-10']
    }],
    order: [{
      id: 'visitors.created_at'
    }],
    timezone: 'America/Los_Angeles'
  }, [
    {
      visitors__created_at_year: '2016-01-01T00:00:00.000Z',
      visitors__visitor_count: '1'
    },
    {
      visitors__created_at_year: '2017-01-01T00:00:00.000Z',
      visitors__visitor_count: '5'
    }
  ]));

  it('minute granularity', () => runQueryTest({
    measures: [
      'visitors.visitor_count'
    ],
    timeDimensions: [{
      dimension: 'visitors.created_at',
      granularity: 'minute',
      dateRange: ['2016-01-09', '2017-01-10']
    }],
    order: [{
      id: 'visitors.created_at'
    }],
    timezone: 'America/Los_Angeles'
  }, [{
    visitors__created_at_minute: '2016-09-06T17:00:00.000Z',
    visitors__visitor_count: '1'
  }, {
    visitors__created_at_minute: '2017-01-02T16:00:00.000Z',
    visitors__visitor_count: '1'
  }, {
    visitors__created_at_minute: '2017-01-04T16:00:00.000Z',
    visitors__visitor_count: '1'
  }, {
    visitors__created_at_minute: '2017-01-05T16:00:00.000Z',
    visitors__visitor_count: '1'
  }, {
    visitors__created_at_minute: '2017-01-06T16:00:00.000Z',
    visitors__visitor_count: '2'
  }]));

  it('second granularity', () => runQueryTest({
    measures: [
      'visitors.visitor_count'
    ],
    timeDimensions: [{
      dimension: 'visitors.created_at',
      granularity: 'second',
      dateRange: ['2016-01-09', '2017-01-10']
    }],
    order: [{
      id: 'visitors.created_at'
    }],
    timezone: 'America/Los_Angeles'
  }, [{
    visitors__created_at_second: '2016-09-06T17:00:00.000Z',
    visitors__visitor_count: '1'
  }, {
    visitors__created_at_second: '2017-01-02T16:00:00.000Z',
    visitors__visitor_count: '1'
  }, {
    visitors__created_at_second: '2017-01-04T16:00:00.000Z',
    visitors__visitor_count: '1'
  }, {
    visitors__created_at_second: '2017-01-05T16:00:00.000Z',
    visitors__visitor_count: '1'
  }, {
    visitors__created_at_second: '2017-01-06T16:00:00.000Z',
    visitors__visitor_count: '2'
  }]));

  it('time date ranges', () => runQueryTest({
    measures: [
      'visitors.visitor_count'
    ],
    timeDimensions: [{
      dimension: 'visitors.created_at',
      granularity: 'day',
      dateRange: ['2017-01-02T15:00:00', '2017-01-02T17:00:00']
    }],
    order: [{
      id: 'visitors.created_at'
    }],
    timezone: 'America/Los_Angeles'
  }, [
    {
      visitors__created_at_day: '2017-01-02T00:00:00.000Z',
      visitors__visitor_count: '1'
    }
  ]));

  it('ungrouped', () => runQueryTest({
    measures: [],
    dimensions: [
      'visitors.id'
    ],
    timeDimensions: [{
      dimension: 'visitors.created_at',
      granularity: 'day',
      dateRange: ['2016-01-09', '2017-01-10']
    }],
    order: [{
      id: 'visitors.created_at'
    }],
    timezone: 'America/Los_Angeles',
    ungrouped: true
  }, [{
    visitors__id: 6,
    visitors__created_at_day: '2016-09-06T00:00:00.000Z'
  }, {
    visitors__id: 1,
    visitors__created_at_day: '2017-01-02T00:00:00.000Z'
  }, {
    visitors__id: 2,
    visitors__created_at_day: '2017-01-04T00:00:00.000Z'
  }, {
    visitors__id: 3,
    visitors__created_at_day: '2017-01-05T00:00:00.000Z'
  }, {
    visitors__id: 4,
    visitors__created_at_day: '2017-01-06T00:00:00.000Z'
  }, {
    visitors__id: 5,
    visitors__created_at_day: '2017-01-06T00:00:00.000Z'
  }]));

  it('offset cache', () => runQueryTest({
    measures: [],
    dimensions: [
      'visitors.id'
    ],
    timeDimensions: [{
      dimension: 'visitors.created_at',
      granularity: 'day',
      dateRange: ['2016-01-09', '2017-01-10']
    }],
    order: [{
      id: 'visitors.created_at'
    }],
    timezone: 'America/Los_Angeles',
    ungrouped: true,
    offset: 5
  }, [{
    visitors__id: 5,
    visitors__created_at_day: '2017-01-06T00:00:00.000Z'
  }]));

  it('ungrouped without id', () => runQueryTest({
    measures: [],
    dimensions: [],
    timeDimensions: [{
      dimension: 'visitors.created_at',
      granularity: 'day',
      dateRange: ['2016-01-09', '2017-01-10']
    }],
    order: [{
      id: 'visitors.created_at'
    }],
    timezone: 'America/Los_Angeles',
    ungrouped: true,
    allowUngroupedWithoutPrimaryKey: true
  }, [{
    visitors__created_at_day: '2016-09-06T00:00:00.000Z'
  }, {
    visitors__created_at_day: '2017-01-02T00:00:00.000Z'
  }, {
    visitors__created_at_day: '2017-01-04T00:00:00.000Z'
  }, {
    visitors__created_at_day: '2017-01-05T00:00:00.000Z'
  }, {
    visitors__created_at_day: '2017-01-06T00:00:00.000Z'
  }, {
    visitors__created_at_day: '2017-01-06T00:00:00.000Z'
  }]));

  it(
    'sqlAlias',
    () => runQueryTest({
      measures: ['CubeWithVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryLongName.count'],
      dimensions: [],
      timeDimensions: [],
      timezone: 'America/Los_Angeles',
      filters: [],
      order: []
    }, [
      { cube_with_long_name__count: '3' }
    ])
  );

  it('data source', async () => {
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: ['CubeWithVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryLongName.count'],
      dimensions: [],
      timeDimensions: [],
      timezone: 'America/Los_Angeles',
      filters: [],
      order: []
    });

    expect(query.dataSource).toEqual('oracle');
  });

  it(
    'objectRestSpread generator',
    () => runQueryTest({
      measures: ['visitors.foo'],
      dimensions: [],
      timeDimensions: [],
      timezone: 'America/Los_Angeles',
      filters: [],
      order: []
    }, [
      { visitors__foo: '6' }
    ])
  );

  it(
    'question mark filter',
    () => runQueryTest({
      measures: ['visitors.visitor_count'],
      dimensions: [],
      timeDimensions: [],
      timezone: 'America/Los_Angeles',
      filters: [{
        or: [{
          member: 'visitors.questionMark',
          operator: 'contains',
          values: ['with some']
        }, {
          member: 'visitors.questionMark',
          operator: 'equals',
          values: [null]
        }, {
          member: 'visitors.questionMark',
          operator: 'equals',
          values: [null, 'with some']
        }]
      }],
      order: []
    }, [
      { visitors__visitor_count: '6' }
    ])
  );

  it(
    'filter group',
    () => runQueryTest({
      measures: ['visitor_checkins.visitor_checkins_count'],
      dimensions: [],
      timeDimensions: [],
      timezone: 'America/Los_Angeles',
      filters: [{
        or: [{
          member: 'visitor_checkins.created_at',
          operator: 'inDateRange',
          values: ['2017-01-01', '2017-01-10']
        }, {
          member: 'visitor_checkins.source',
          operator: 'equals',
          values: ['google_123_123']
        }]
      }],
      order: []
    }, [
      { vc__visitor_checkins_count: '4' }
    ])
  );

  it(
    'filter group sub filter',
    () => runQueryTest({
      measures: ['visitor_checkins.visitor_checkins_count'],
      dimensions: [],
      timeDimensions: [],
      timezone: 'America/Los_Angeles',
      filters: [{
        and: [{
          or: [{
            member: 'visitor_checkins.created_at',
            operator: 'inDateRange',
            values: ['2017-01-01', '2017-01-10']
          }, {
            member: 'visitor_checkins.source',
            operator: 'equals',
            values: ['google_123_123']
          }]
        }, {
          member: 'visitor_checkins.visitor_id',
          operator: 'gte',
          values: ['1']
        }]
      }, {
        member: 'visitor_checkins.visitor_id',
        operator: 'lte',
        values: ['100']
      }],
      order: []
    }, [
      { vc__visitor_checkins_count: '4' }
    ])
  );

  it(
    'filter group simple filter',
    () => runQueryTest({
      measures: ['visitor_checkins.visitor_checkins_count'],
      dimensions: [],
      timeDimensions: [],
      timezone: 'America/Los_Angeles',
      filters: [{
        member: 'visitor_checkins.created_at',
        operator: 'inDateRange',
        values: ['2017-01-01', '2017-01-10']
      }],
      order: []
    }, [
      { vc__visitor_checkins_count: '4' }
    ])
  );

  it(
    'filter group double tree',
    () => runQueryTest({
      measures: ['visitor_checkins.visitor_checkins_count'],
      dimensions: [],
      timeDimensions: [],
      timezone: 'America/Los_Angeles',
      filters: [{
        or: [{
          and: [{
            member: 'visitor_checkins.created_at',
            operator: 'inDateRange',
            values: ['2017-01-01', '2017-01-10']
          }, {
            member: 'visitor_checkins.source',
            operator: 'equals',
            values: ['google']
          }]
        }, {
          and: [{
            member: 'visitor_checkins.created_at',
            operator: 'inDateRange',
            values: ['2017-01-05', '2017-01-10']
          }, {
            member: 'visitor_checkins.source',
            operator: 'equals',
            values: [null]
          }]
        }]
      }],
      order: []
    }, [
      { vc__visitor_checkins_count: '1' }
    ])
  );

  it(
    'filter group double tree and non matching filter',
    () => runQueryTest({
      measures: ['visitor_checkins.visitor_checkins_count'],
      dimensions: [],
      timeDimensions: [],
      timezone: 'America/Los_Angeles',
      filters: [{
        or: [{
          and: [{
            member: 'visitor_checkins.created_at',
            operator: 'inDateRange',
            values: ['2017-01-01', '2017-01-10']
          }, {
            member: 'visitor_checkins.source',
            operator: 'equals',
            values: ['google']
          }]
        }, {
          and: [{
            member: 'visitor_checkins.created_at',
            operator: 'inDateRange',
            values: ['2017-01-05', '2017-01-10']
          }, {
            member: 'visitor_checkins.source',
            operator: 'equals',
            values: [null]
          }]
        }]
      }, {
        member: 'visitor_checkins.visitor_id',
        operator: 'equals',
        values: ['1']
      }],
      order: []
    }, [
      { vc__visitor_checkins_count: '1' }
    ])
  );

  const baseQuery = {
    measures: [
      'visitors.countDistinctApproxRolling'
    ],
    filters: [],
    timeDimensions: [],
    order: [{
      id: 'visitors.created_at'
    }],
    timezone: 'America/Los_Angeles'
  };

  const granularityCases = [
    {
      granularity: 'day',
      from: '2017-01-01T00:00:00.000000',
      to: '2017-01-10T23:59:59.999999'
    },
    {
      granularity: 'week',
      from: '2016-12-26T00:00:00.000000',
      to: '2017-01-15T23:59:59.999999'
    },
    {
      granularity: 'month',
      from: '2017-01-01T00:00:00.000000',
      to: '2017-01-31T23:59:59.999999'
    },
    {
      granularity: 'year',
      from: '2017-01-01T00:00:00.000000',
      to: '2017-12-31T23:59:59.999999'
    }
  ];

  // eslint-disable-next-line
  for (const granularityTest of granularityCases) {
    // eslint-disable-next-line no-loop-func
    it(`Should date with TZ, when pass timeDimensions with granularity by ${granularityTest.granularity}`, async () => {
      if (getEnv('nativeSqlPlanner')) {
        return;
      }
      await compiler.compile();

      const query = new BigqueryQuery({ joinGraph, cubeEvaluator, compiler }, {
        ...baseQuery,
        timeDimensions: [{
          dimension: 'visitors.created_at',
          granularity: granularityTest.granularity,
          dateRange: ['2017-01-01', '2017-01-10']
        }]
      });

      const sqlBuild = query.buildSqlAndParams();

      console.log(sqlBuild[0]);
      console.log(sqlBuild[1]);

      expect(sqlBuild[0].includes('America/Los_Angeles')).toEqual(true);
      expect(sqlBuild[1][0]).toEqual(granularityTest.from);
      expect(sqlBuild[1][1]).toEqual(granularityTest.to);
    });
  }

  it('compound key count', async () => runQueryTest(
    {
      measures: ['compound.count'],
      timeDimensions: [
      ],
      timezone: 'America/Los_Angeles',
      filters: [
        {
          dimension: 'visitor_checkins.revenue_per_checkin',
          operator: 'gte',
          values: ['10'],
        },
      ],
    },
    [{ compound__count: '4' }]
  ));

  it('compound key self join', async () => runQueryTest(
    {
      measures: ['compound.rank_avg'],
      timeDimensions: [
        {
          dimension: 'visitors.created_at',
          granularity: 'day',
          dateRange: ['2017-01-01', '2017-01-30'],
        },
      ],
      timezone: 'America/Los_Angeles',
      filters: [
        {
          dimension: 'visitor_checkins.revenue_per_checkin',
          operator: 'gte',
          values: ['10'],
        },
      ],
    },
    [
      { compound__rank_avg: '7.5000000000000000', visitors__created_at_day: '2017-01-02T00:00:00.000Z' },
      { compound__rank_avg: '7.5000000000000000', visitors__created_at_day: '2017-01-04T00:00:00.000Z' },
    ]
  ));

  it('rank measure', async () => runQueryTest(
    {
      measures: ['visitors.revenue_rank'],
    },
    [{ visitors__revenue_rank: '1' }]
  ));

  it('rank measure with dimension', async () => runQueryTest(
    {
      measures: ['visitors.revenue_rank'],
      dimensions: ['visitors.source'],
    },
    [{
      visitors__revenue_rank: '2',
      visitors__source: null
    }, {
      visitors__revenue_rank: '2',
      visitors__source: 'google'
    }, {
      visitors__revenue_rank: '1',
      visitors__source: 'some'
    }]
  ));

  it('multi stage measure with multiple dependencies', async () => runQueryTest(
    {
      measures: ['visitors.second_rank_sum', 'visitors.visitor_revenue', 'visitors.revenue_rank'],
      dimensions: ['visitors.source'],
    },
    [{
      visitors__revenue_rank: '2',
      visitors__second_rank_sum: null,
      visitors__source: null,
      visitors__visitor_revenue: null,
    }, {
      visitors__revenue_rank: '2',
      visitors__second_rank_sum: null,
      visitors__source: 'google',
      visitors__visitor_revenue: null,
    }, {
      visitors__revenue_rank: '1',
      visitors__second_rank_sum: '300',
      visitors__source: 'some',
      visitors__visitor_revenue: '300',
    }]
  ));

  it('multi stage complex graph', async () => runQueryTest(
    {
      measures: ['visitors.adjusted_rank_sum', 'visitors.visitor_revenue'],
      dimensions: ['visitors.source'],
      order: [{
        id: 'visitors.source'
      }],
    },
    [{
      visitors__source: 'google',
      visitors__adjusted_rank_sum: null,
      visitors__visitor_revenue: null,
    }, {
      visitors__source: 'some',
      visitors__adjusted_rank_sum: '100.1',
      visitors__visitor_revenue: '300'
    }, {
      visitors__source: null,
      visitors__adjusted_rank_sum: null,
      visitors__visitor_revenue: null,
    }]
  ));

  it('multi stage complex graph with time dimension', async () => runQueryTest(
    {
      measures: ['visitors.adjusted_rank_sum', 'visitors.visitor_revenue'],
      dimensions: ['visitors.source'],
      timeDimensions: [
        {
          dimension: 'visitors.updated_at',
          granularity: 'day',
        },
      ],
      order: [{
        id: 'visitors.source'
      }],
      timezone: 'UTC',
    },
    [{
      visitors__source: 'google',
      visitors__updated_at_day: '2017-01-20T00:00:00.000Z',
      visitors__adjusted_rank_sum: null,
      visitors__visitor_revenue: null
    }, {
      visitors__source: 'some',
      visitors__updated_at_day: '2017-01-15T00:00:00.000Z',
      visitors__adjusted_rank_sum: '200.1',
      visitors__visitor_revenue: '200'
    }, {
      visitors__source: 'some',
      visitors__updated_at_day: '2017-01-30T00:00:00.000Z',
      visitors__adjusted_rank_sum: '100.1',
      visitors__visitor_revenue: '100'
    }, {
      visitors__source: null,
      visitors__updated_at_day: '2016-09-07T00:00:00.000Z',
      visitors__adjusted_rank_sum: null,
      visitors__visitor_revenue: null
    }, {
      visitors__source: null,
      visitors__updated_at_day: '2017-01-25T00:00:00.000Z',
      visitors__adjusted_rank_sum: null,
      visitors__visitor_revenue: null
    }]
  ));

  if (!getEnv('nativeSqlPlanner')) {
    it('multi stage revenue_sum_group_by_granularity and group by td with granularity', async () => runQueryTest(
      {
        measures: ['visitors.revenue_sum_group_by_granularity'],
        dimensions: ['visitors.source'],
        order: [{
          id: 'visitors.source'
        }],
        timezone: 'UTC',
      },
      [{
        visitors__revenue_sum_group_by_granularity: '300',
        visitors__source: 'google',
      },
      {
        visitors__revenue_sum_group_by_granularity: '300',
        visitors__source: 'some',
      },
      {
        visitors__revenue_sum_group_by_granularity: '900',
        visitors__source: null,
      },
      {
        visitors__revenue_sum_group_by_granularity: '500',
        visitors__source: null,
      }]
    ));
  } else {
    it.skip('FIXME(tesseract): multi stage revenue_sum_group_by_granularity and group by td with granularity', () => {
      // Should be fixed in tesseract
    });
  }

  it('multi stage complex graph with time dimension no granularity', async () => runQueryTest(
    {
      measures: ['visitors.adjusted_rank_sum', 'visitors.visitor_revenue'],
      dimensions: ['visitors.source'],
      timeDimensions: [
        {
          dimension: 'visitors.updated_at',
          dateRange: ['2017-01-01', '2017-01-30'],
        },
      ],
      order: [{
        id: 'visitors.source'
      }],
      timezone: 'UTC',
    },
    [{
      visitors__source: 'google',
      visitors__adjusted_rank_sum: null,
      visitors__visitor_revenue: null
    }, {
      visitors__source: 'some',
      visitors__adjusted_rank_sum: '100.1',
      visitors__visitor_revenue: '300'
    }, {
      visitors__source: null,
      visitors__adjusted_rank_sum: null,
      visitors__visitor_revenue: null
    }]
  ));

  it('multi stage complex graph with time dimension no granularity raw dimension', async () => runQueryTest(
    {
      measures: ['visitors.adjusted_rank_sum', 'visitors.visitor_revenue'],
      dimensions: ['visitors.source', 'visitors.updated_at'],
      timeDimensions: [
        {
          dimension: 'visitors.updated_at',
          dateRange: ['2017-01-01', '2017-01-30'],
        },
      ],
      order: [{
        id: 'visitors.source'
      }],
      timezone: 'UTC',
    },
    [{
      visitors__source: 'google',
      visitors__updated_at: '2017-01-20T00:00:00.000Z',
      visitors__adjusted_rank_sum: null,
      visitors__visitor_revenue: null
    }, {
      visitors__source: 'some',
      visitors__updated_at: '2017-01-15T00:00:00.000Z',
      visitors__adjusted_rank_sum: '200.1',
      visitors__visitor_revenue: '200'
    }, {
      visitors__source: 'some',
      visitors__updated_at: '2017-01-30T00:00:00.000Z',
      visitors__adjusted_rank_sum: '100.1',
      visitors__visitor_revenue: '100'
    }, {
      visitors__source: null,
      visitors__updated_at: '2017-01-25T00:00:00.000Z',
      visitors__adjusted_rank_sum: null,
      visitors__visitor_revenue: null
    }]
  ));

  it('multi stage complex graph with time dimension through view', async () => runQueryTest(
    {
      measures: ['visitors_multi_stage.adjusted_rank_sum', 'visitors_multi_stage.visitor_revenue'],
      dimensions: ['visitors_multi_stage.source'],
      timeDimensions: [
        {
          dimension: 'visitors_multi_stage.updated_at',
          granularity: 'day',
        },
      ],
      order: [{
        id: 'visitors_multi_stage.source'
      }],
      timezone: 'UTC',
    },
    [{
      visitors_multi_stage__source: 'google',
      visitors_multi_stage__updated_at_day: '2017-01-20T00:00:00.000Z',
      visitors_multi_stage__adjusted_rank_sum: null,
      visitors_multi_stage__visitor_revenue: null
    }, {
      visitors_multi_stage__source: 'some',
      visitors_multi_stage__updated_at_day: '2017-01-15T00:00:00.000Z',
      visitors_multi_stage__adjusted_rank_sum: '200.1',
      visitors_multi_stage__visitor_revenue: '200'
    }, {
      visitors_multi_stage__source: 'some',
      visitors_multi_stage__updated_at_day: '2017-01-30T00:00:00.000Z',
      visitors_multi_stage__adjusted_rank_sum: '100.1',
      visitors_multi_stage__visitor_revenue: '100'
    }, {
      visitors_multi_stage__source: null,
      visitors_multi_stage__updated_at_day: '2016-09-07T00:00:00.000Z',
      visitors_multi_stage__adjusted_rank_sum: null,
      visitors_multi_stage__visitor_revenue: null
    }, {
      visitors_multi_stage__source: null,
      visitors_multi_stage__updated_at_day: '2017-01-25T00:00:00.000Z',
      visitors_multi_stage__adjusted_rank_sum: null,
      visitors_multi_stage__visitor_revenue: null
    }]
  ));

  it('multi stage percentage of total', async () => runQueryTest(
    {
      measures: ['visitors.revenue', 'visitors.percentage_of_total'],
      dimensions: ['visitors.source'],
      order: [{
        id: 'visitors.source'
      }],
    },
    [{
      visitors__percentage_of_total: 15,
      visitors__revenue: '300',
      visitors__source: 'google'
    }, {
      visitors__percentage_of_total: 15,
      visitors__revenue: '300',
      visitors__source: 'some'
    }, {
      visitors__percentage_of_total: 70,
      visitors__revenue: '1400',
      visitors__source: null
    }]
  ));

  it('multi stage percentage of total with limit', async () => runQueryTest(
    {
      measures: ['visitors_multi_stage.percentage_of_total'],
      dimensions: ['visitors_multi_stage.source'],
      order: [{
        id: 'visitors_multi_stage.source'
      }],
      rowLimit: 1,
      limit: 1
    },
    [{
      visitors_multi_stage__percentage_of_total: 15,
      visitors_multi_stage__source: 'google'
    }]
  ));

  it('multi stage percentage of total with limit totals', async () => runQueryTest(
    {
      measures: ['visitors_multi_stage.percentage_of_total'],
      rowLimit: 1
    },
    [{
      visitors_multi_stage__percentage_of_total: 100
    }]
  ));

  it('multi stage percentage of total filtered', async () => runQueryTest(
    {
      measures: ['visitors.revenue', 'visitors.percentage_of_total'],
      dimensions: ['visitors.source'],
      order: [{
        id: 'visitors.source'
      }],
      filters: [{
        dimension: 'visitors.id',
        operator: 'equals',
        values: ['1', '2', '3', '4']
      }],
    },
    [{
      visitors__percentage_of_total: 30,
      visitors__revenue: '300',
      visitors__source: 'google'
    }, {
      visitors__percentage_of_total: 30,
      visitors__revenue: '300',
      visitors__source: 'some'
    }, {
      visitors__percentage_of_total: 40,
      visitors__revenue: '400',
      visitors__source: null
    }]
  ));

  it('multi stage percentage of total filtered with time dimension', async () => runQueryTest(
    {
      measures: ['visitors.revenue', 'visitors.percentage_of_total'],
      dimensions: ['visitors.source'],
      order: [{
        id: 'visitors.source'
      }, {
        id: 'visitors.created_at'
      }],
      filters: [{
        dimension: 'visitors.id',
        operator: 'equals',
        values: ['1', '2', '3', '4']
      }],
      timeDimensions: [
        {
          dimension: 'visitors.created_at',
          granularity: 'day',
          dateRange: ['2017-01-01', '2017-01-30'],
        },
      ],
      timezone: 'America/Los_Angeles',
    },
    [{
      visitors__created_at_day: '2017-01-05T00:00:00.000Z',
      visitors__percentage_of_total: 30,
      visitors__revenue: '300',
      visitors__source: 'google'
    }, {
      visitors__created_at_day: '2017-01-02T00:00:00.000Z',
      visitors__percentage_of_total: 10,
      visitors__revenue: '100',
      visitors__source: 'some'
    }, {
      visitors__created_at_day: '2017-01-04T00:00:00.000Z',
      visitors__percentage_of_total: 20,
      visitors__revenue: '200',
      visitors__source: 'some'
    }, {
      visitors__created_at_day: '2017-01-06T00:00:00.000Z',
      visitors__percentage_of_total: 40,
      visitors__revenue: '400',
      visitors__source: null
    }]
  ));

  it('multi stage percentage of total filtered and joined', async () => runQueryTest(
    {
      measures: ['visitors.revenue', 'visitors.percentage_of_total'],
      dimensions: ['visitor_checkins.source'],
      order: [{
        id: 'visitor_checkins.source'
      }],
      filters: [{
        dimension: 'visitors.id',
        operator: 'equals',
        values: ['1', '2', '3', '4']
      }],
    },
    [{
      visitors__percentage_of_total: 9,
      visitors__revenue: '100',
      vc__source: 'google'
    }, {
      visitors__percentage_of_total: 91,
      visitors__revenue: '1000',
      vc__source: null
    }]
  ));

  it('multi stage sum with group by', async () => runQueryTest(
    {
      measures: ['visitors.visitors_revenue_per_source', 'visitors.revenue'],
      dimensions: ['visitors.source', 'visitors.created_at'],
      order: [{
        id: 'visitors.source'
      }, {
        id: 'visitors.created_at'
      }],
    },
    [{
      visitors__source: 'google',
      visitors__created_at: '2017-01-06T00:00:00.000Z',
      visitors__visitors_revenue_per_source: '300',
      visitors__revenue: '300'
    },
    {
      visitors__source: 'some',
      visitors__created_at: '2017-01-03T00:00:00.000Z',
      visitors__visitors_revenue_per_source: '300',
      visitors__revenue: '100'
    },
    {
      visitors__source: 'some',
      visitors__created_at: '2017-01-05T00:00:00.000Z',
      visitors__visitors_revenue_per_source: '300',
      visitors__revenue: '200'
    },
    {
      visitors__source: null,
      visitors__created_at: '2016-09-07T00:00:00.000Z',
      visitors__visitors_revenue_per_source: '1400',
      visitors__revenue: '500'
    },
    {
      visitors__source: null,
      visitors__created_at: '2017-01-07T00:00:00.000Z',
      visitors__visitors_revenue_per_source: '1400',
      visitors__revenue: '900'
    }]
  ));

  if (getEnv('nativeSqlPlanner')) {
    it('multi stage sum with group by over view', async () => runQueryTest(
      {
        measures: ['visitors_multi_stage.visitors_revenue_per_source', 'visitors_multi_stage.revenue'],
        dimensions: ['visitors_multi_stage.source', 'visitors_multi_stage.created_at'],
        order: [{
          id: 'visitors_multi_stage.source'
        }, {
          id: 'visitors_multi_stage.created_at'
        }],
      },
      [{
        visitors_multi_stage__source: 'google',
        visitors_multi_stage__created_at: '2017-01-06T00:00:00.000Z',
        visitors_multi_stage__visitors_revenue_per_source: '300',
        visitors_multi_stage__revenue: '300'
      },
      {
        visitors_multi_stage__source: 'some',
        visitors_multi_stage__created_at: '2017-01-03T00:00:00.000Z',
        visitors_multi_stage__visitors_revenue_per_source: '300',
        visitors_multi_stage__revenue: '100'
      },
      {
        visitors_multi_stage__source: 'some',
        visitors_multi_stage__created_at: '2017-01-05T00:00:00.000Z',
        visitors_multi_stage__visitors_revenue_per_source: '300',
        visitors_multi_stage__revenue: '200'
      },
      {
        visitors_multi_stage__source: null,
        visitors_multi_stage__created_at: '2016-09-07T00:00:00.000Z',
        visitors_multi_stage__visitors_revenue_per_source: '1400',
        visitors_multi_stage__revenue: '500'
      },
      {
        visitors_multi_stage__source: null,
        visitors_multi_stage__created_at: '2017-01-07T00:00:00.000Z',
        visitors_multi_stage__visitors_revenue_per_source: '1400',
        visitors_multi_stage__revenue: '900'
      }]
    ));
  } else {
    it.skip('multi stage sum with reduce by over view', async () => {
    // Works only in Tesseract
    });
  }

  it('multi stage sum with reduce by', async () => runQueryTest(
    {
      measures: ['visitors.visitors_revenue_without_date', 'visitors.revenue'],
      dimensions: ['visitors.source', 'visitors.created_at'],
      order: [{
        id: 'visitors.source'
      }, {
        id: 'visitors.created_at'
      }],
    },
    [{
      visitors__source: 'google',
      visitors__created_at: '2017-01-06T00:00:00.000Z',
      visitors__visitors_revenue_without_date: '300',
      visitors__revenue: '300'
    },
    {
      visitors__source: 'some',
      visitors__created_at: '2017-01-03T00:00:00.000Z',
      visitors__visitors_revenue_without_date: '300',
      visitors__revenue: '100'
    },
    {
      visitors__source: 'some',
      visitors__created_at: '2017-01-05T00:00:00.000Z',
      visitors__visitors_revenue_without_date: '300',
      visitors__revenue: '200'
    },
    {
      visitors__source: null,
      visitors__created_at: '2016-09-07T00:00:00.000Z',
      visitors__visitors_revenue_without_date: '1400',
      visitors__revenue: '500'
    },
    {
      visitors__source: null,
      visitors__created_at: '2017-01-07T00:00:00.000Z',
      visitors__visitors_revenue_without_date: '1400',
      visitors__revenue: '900'
    }]
  ));

  if (getEnv('nativeSqlPlanner')) {
    it('multi stage sum with reduce by over view', async () => runQueryTest(
      {
        measures: ['visitors_multi_stage.visitors_revenue_without_date', 'visitors_multi_stage.revenue'],
        dimensions: ['visitors_multi_stage.source', 'visitors_multi_stage.created_at'],
        order: [{
          id: 'visitors_multi_stage.source'
        }, {
          id: 'visitors_multi_stage.created_at'
        }],
      },
      [{
        visitors_multi_stage__source: 'google',
        visitors_multi_stage__created_at: '2017-01-06T00:00:00.000Z',
        visitors_multi_stage__visitors_revenue_without_date: '300',
        visitors_multi_stage__revenue: '300'
      },
      {
        visitors_multi_stage__source: 'some',
        visitors_multi_stage__created_at: '2017-01-03T00:00:00.000Z',
        visitors_multi_stage__visitors_revenue_without_date: '300',
        visitors_multi_stage__revenue: '100'
      },
      {
        visitors_multi_stage__source: 'some',
        visitors_multi_stage__created_at: '2017-01-05T00:00:00.000Z',
        visitors_multi_stage__visitors_revenue_without_date: '300',
        visitors_multi_stage__revenue: '200'
      },
      {
        visitors_multi_stage__source: null,
        visitors_multi_stage__created_at: '2016-09-07T00:00:00.000Z',
        visitors_multi_stage__visitors_revenue_without_date: '1400',
        visitors_multi_stage__revenue: '500'
      },
      {
        visitors_multi_stage__source: null,
        visitors_multi_stage__created_at: '2017-01-07T00:00:00.000Z',
        visitors_multi_stage__visitors_revenue_without_date: '1400',
        visitors_multi_stage__revenue: '900'
      }]
    ));
  } else {
    it.skip('multi stage sum with reduce by over view', async () => {
    // Works only in Tesseract
    });
  }

  it('multiplied sum and count no dimensions through view', async () => runQueryTest(
    {
      measures: ['visitors_visitors_checkins_view.revenue', 'visitors_visitors_checkins_view.visitor_checkins_count'],
    },
    [{
      visitors_visitors_checkins_view__revenue: '2000',
      visitors_visitors_checkins_view__visitor_checkins_count: '6'
    }]
  ));

  it('multiplied sum no dimensions through view', async () => runQueryTest(
    {
      measures: ['visitors_visitors_checkins_view.revenue', 'visitors_visitors_checkins_view.id_sum'],
    },
    [{
      visitors_visitors_checkins_view__revenue: '2000',
      visitors_visitors_checkins_view__id_sum: '21'
    }]
  ));

  if (getEnv('nativeSqlPlanner')) {
    it('nested aggregations with filtered measures and rolling windows', async () => runQueryTest(
      {
        measures: ['rollingWindowTest.revenue_ms'],
      },
      [{
        rolling_window_test__revenue_ms: '12'
      }]
    ));
  }

  it('multiplied sum and count no dimensions through view', async () => runQueryTest(
    {
      measures: ['visitors_visitors_checkins_view.revenue', 'visitors_visitors_checkins_view.visitor_checkins_count'],
    },
    [{
      visitors_visitors_checkins_view__revenue: '2000',
      visitors_visitors_checkins_view__visitor_checkins_count: '6'
    }]
  ));

  it('multiplied sum no dimensions through view', async () => runQueryTest(
    {
      measures: ['visitors_visitors_checkins_view.revenue', 'visitors_visitors_checkins_view.id_sum'],
    },
    [{
      visitors_visitors_checkins_view__revenue: '2000',
      visitors_visitors_checkins_view__id_sum: '21'
    }]
  ));

  it('aliases for columns with digits and underscores', async () => runQueryTest(
    {
      measures: ['visitors.revenue_1d_d1_dd'],
    },
    [{
      visitors__revenue_1d_d1_dd: '2000',
    }]
  ));

  it('simple join with segment', async () => runQueryTest(
    {
      measures: [
        'visitors.visitor_revenue',
        'visitors.visitor_count',
        'visitor_checkins.visitor_checkins_count',
        'visitors.per_visitor_revenue'
      ],
      timeDimensions: [{
        dimension: 'visitors.created_at',
        granularity: 'day',
        dateRange: ['2017-01-01', '2017-01-30']
      }],
      segments: ['visitors.some_source'],
      timezone: 'America/Los_Angeles',
      order: [{
        id: 'visitors.created_at'
      }]
    },
    [
      {
        visitors__created_at_day: '2017-01-02T00:00:00.000Z',
        visitors__visitor_revenue: '100',
        visitors__visitor_count: '1',
        vc__visitor_checkins_count: '3',
        visitors__per_visitor_revenue: '100'
      },
      {
        visitors__created_at_day: '2017-01-04T00:00:00.000Z',
        visitors__visitor_revenue: '200',
        visitors__visitor_count: '1',
        vc__visitor_checkins_count: '2',
        visitors__per_visitor_revenue: '200'
      },
    ]
  ));

  // Subquery aggregation for multiplied measure (and any `keysSelect` for that matter)
  // should pick up all dimensions, even through member expressions
  it('multiplied sum with dimension member expressions', async () => runQueryTest(
    {
      measures: [
        'visitors_visitors_checkins_view.revenue',
        'visitors_visitors_checkins_view.visitor_checkins_count',
      ],
      dimensions: [
        {
          // eslint-disable-next-line no-new-func
          expression: new Function(
            'visitors_visitors_checkins_view',
            // eslint-disable-next-line no-template-curly-in-string
            'return `LOWER(${visitors_visitors_checkins_view.source})`'
          ),
          expressionName: 'lower_source',
          // eslint-disable-next-line no-template-curly-in-string
          definition: 'LOWER(${visitors_visitors_checkins_view.source})',
          cubeName: 'visitors_visitors_checkins_view',
        },
        {
          // eslint-disable-next-line no-new-func
          expression: new Function(
            'visitors_visitors_checkins_view',
            // eslint-disable-next-line no-template-curly-in-string
            'return `UPPER(${visitors_visitors_checkins_view.source})`'
          ),
          expressionName: 'upper_source',
          // eslint-disable-next-line no-template-curly-in-string
          definition: 'UPPER(${visitors_visitors_checkins_view.source})',
          cubeName: 'visitors_visitors_checkins_view',
        },
      ],
    },
    [
      {
        lower_source: null,
        upper_source: null,
        visitors_visitors_checkins_view__revenue: '1400',
        visitors_visitors_checkins_view__visitor_checkins_count: '0',
      },
      {
        lower_source: 'google',
        upper_source: 'GOOGLE',
        visitors_visitors_checkins_view__revenue: '300',
        visitors_visitors_checkins_view__visitor_checkins_count: '1',
      },
      {
        lower_source: 'some',
        upper_source: 'SOME',
        visitors_visitors_checkins_view__revenue: '300',
        visitors_visitors_checkins_view__visitor_checkins_count: '5',
      },
    ]
  ));

  // TODO not implemented
  // it('multi stage bucketing', async () => runQueryTest(
  //   {
  //     measures: ['visitors.revenue'],
  //     dimensions: ['visitors.revenue_bucket'],
  //     order: [{
  //       id: 'visitors.revenue_bucket'
  //     }],
  //   },
  //   [{
  //     visitors__percentage_of_total: 15,
  //     visitors__revenue: '300',
  //     visitors__source: 'google'
  //   }, {
  //     visitors__percentage_of_total: 15,
  //     visitors__revenue: '300',
  //     visitors__source: 'some'
  //   }, {
  //     visitors__percentage_of_total: 70,
  //     visitors__revenue: '1400',
  //     visitors__source: null
  //   }]
  // ));

  it('columns order for the query with the sub-query', async () => {
    const joinedSchemaCompilers = prepareJsCompiler(createJoinedCubesSchema());
    await joinedSchemaCompilers.compiler.compile();
    const query = new PostgresQuery({
      joinGraph: joinedSchemaCompilers.joinGraph,
      cubeEvaluator: joinedSchemaCompilers.cubeEvaluator,
      compiler: joinedSchemaCompilers.compiler,
    },
    {
      measures: ['B.bval_sum', 'B.count'],
      dimensions: ['B.aid'],
      filters: [{
        member: 'C.did',
        operator: 'lt',
        values: ['10']
      }],
      order: [{
        'B.bval_sum': 'desc'
      }]
    });
    const sql = query.buildSqlAndParams();
    return dbRunner
      .testQuery(sql)
      .then((res) => {
        res.forEach((row) => {
          const cols = Object.keys(row);
          expect(cols[0]).toEqual('b__aid');
          expect(cols[1]).toEqual('b__bval_sum');
          expect(cols[2]).toEqual('b__count');
        });
      });
  });

  it('don\'t use COALESCE with single argument', async () => {
    if (!getEnv('nativeSqlPlanner')) {
      return;
    }
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitors.visitor_revenue'
      ],
      dimensions: [
        'visitors.source'
      ],
      timeDimensions: [],
      timezone: 'America/Los_Angeles',
      filters: [{
        dimension: 'visitor_checkins.source',
        operator: 'equals',
        values: ['google']
      }],
      order: [{
        id: 'visitors.source'
      }]
    });
    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);
    expect(queryAndParams[0]).not.toContain('COALESCE');

    await dbRunner.testQuery(queryAndParams).then(res => {
      console.log(JSON.stringify(res));
      expect(res).toEqual(
        [{
          visitors__source: 'some',
          visitors__visitor_revenue: '100'
        }]
      );
    });
  });

  it('expression cube name cache', async () => {
    await runQueryTest(
      {
        dimensions: [{
          // eslint-disable-next-line no-new-func,no-template-curly-in-string
          expression: new Function('visitors', 'return `CASE WHEN ${visitors.id} > 10 THEN 10 ELSE 0 END`'),
          expressionName: 'visitors.id_case',
          // eslint-disable-next-line no-template-curly-in-string
          definition: 'CASE WHEN ${visitors.id} > 10 THEN 10 ELSE 0 END',
          cubeName: 'visitors'
        }],
      },
      [{ visitors__id_case: 0 }]
    );

    await runQueryTest(
      {
        dimensions: [{
          // eslint-disable-next-line no-new-func,no-template-curly-in-string
          expression: new Function('visitor_checkins', 'return `CASE WHEN ${visitor_checkins.id} > 10 THEN 10 ELSE 0 END`'),
          expressionName: 'visitors.id_case',
          // eslint-disable-next-line no-template-curly-in-string
          definition: 'CASE WHEN ${visitor_checkins.id} > 10 THEN 10 ELSE 0 END',
          cubeName: 'visitors'
        }],
      },
      [{ visitors__id_case: 0 }]
    );
  });

  it('ungrouped measure with filter', async () => runQueryTest({
    measures: [
      'UngroupedMeasureWithFilter_View.sum_filter',
      'UngroupedMeasureWithFilter_View.count'
    ],
    ungrouped: true,
    allowUngroupedWithoutPrimaryKey: true,
  }, [{
    ungrouped_measure_with_filter__view__count: 1,
    ungrouped_measure_with_filter__view__sum_filter: 1
  }]));

  it('patched measure expression', async () => {
    await runQueryTest(
      {
        measures: [
          'visitors.revenue',
          'visitors.visitor_revenue',
          {
            expression: {
              type: 'PatchMeasure',
              sourceMeasure: 'visitors.revenue',
              replaceAggregationType: 'max',
              addFilters: [],
            },
            cubeName: 'visitors',
            name: 'max_revenue',
            definition: 'PatchMeasure(visitors.revenue, max, [])',
          },
          {
            expression: {
              type: 'PatchMeasure',
              sourceMeasure: 'visitors.revenue',
              replaceAggregationType: null,
              addFilters: [
                {
                  sql: (visitors) => `${visitors.source} IN ('google', 'some')`,
                },
              ],
            },
            cubeName: 'visitors',
            name: 'google_revenue',
            // eslint-disable-next-line no-template-curly-in-string
            definition: 'PatchMeasure(visitors.revenue, min, [${visitors.source} IN (\'google\', \'some\')])',
          },
        ],
      },
      [{
        visitors__revenue: '2000',
        visitors__visitor_revenue: '300',
        visitors__max_revenue: 500,
        visitors__google_revenue: '600',
      }]
    );
  });

  describe('Transitive join paths', () => {
    // eslint-disable-next-line @typescript-eslint/no-shadow
    const { compiler, joinGraph, cubeEvaluator } =
      // language=yaml
      prepareYamlCompiler(`
cubes:
  - name: merchant_dims
    sql: |
      (
        SELECT 101 AS merchant_sk, 'M1' AS merchant_id
        UNION ALL
        SELECT 102 AS merchant_sk, 'M2' AS merchant_id
      )
    dimensions:
      - name: merchant_sk
        sql: merchant_sk
        type: number
        primary_key: true
      - name: merchant_id
        sql: merchant_id
        type: string

  - name: product_dims
    sql: |
      (
        SELECT 201 AS product_sk, 'P1' AS product_id
        UNION ALL
        SELECT 202 AS product_sk, 'P2' AS product_id
      )
    dimensions:
      - name: product_sk
        sql: product_sk
        type: number
        primary_key: true
      - name: product_id
        sql: product_id
        type: string

  - name: merchant_and_product_dims
    sql: |
      (
        SELECT 'M1' AS merchant_id, 'P1' AS product_id, 'Organic' AS acquisition_channel
        UNION ALL
        SELECT 'M1' AS merchant_id, 'P2' AS product_id, 'Paid' AS acquisition_channel
        UNION ALL
        SELECT 'M2' AS merchant_id, 'P1' AS product_id, 'Referral' AS acquisition_channel
      )
    dimensions:
      - name: product_id
        sql: product_id
        type: string
        primary_key: true
      - name: merchant_id
        sql: merchant_id
        type: string
        primary_key: true
      - name: acquisition_channel
        sql: acquisition_channel
        type: string

  - name: test_facts
    sql: |
      (
        SELECT DATE '2023-01-01' AS reporting_date, 101 AS merchant_sk, 201 AS product_sk, 100 AS amount
        UNION ALL
        SELECT DATE '2023-01-01' AS reporting_date, 101 AS merchant_sk, 202 AS product_sk, 150 AS amount
        UNION ALL
        SELECT DATE '2023-01-02' AS reporting_date, 102 AS merchant_sk, 201 AS product_sk, 200 AS amount
      )
    joins:
      - name: merchant_dims
        relationship: many_to_one
        sql: "{CUBE}.merchant_sk = {merchant_dims.merchant_sk}"
      - name: product_dims
        relationship: many_to_one
        sql: "{CUBE}.product_sk = {product_dims.product_sk}"
      - name: merchant_and_product_dims # This join depends on merchant_dims and product_dims
        relationship: many_to_one
        sql: "{merchant_dims.merchant_id} = {merchant_and_product_dims.merchant_id} AND {product_dims.product_id} = {merchant_and_product_dims.product_id}"
    dimensions:
      - name: reporting_date
        sql: reporting_date
        type: time
        primary_key: true
      - name: merchant_sk
        sql: merchant_sk
        type: number
        primary_key: true
      - name: product_sk
        sql: product_sk
        type: number
        primary_key: true
      - name: acquisition_channel # This dimension triggers the join to merchant_and_product_dims
        sql: "{merchant_and_product_dims.acquisition_channel}"
        type: string
    measures:
      - name: amount_sum
        sql: amount
        type: sum

# Join loop for testing transitive joins
  - name: alpha_facts
    sql: |
      (
        SELECT DATE '2023-01-01' AS reporting_date, 1 AS a_id, 10 AS b_id, 100 AS amount
        UNION ALL
        SELECT DATE '2023-01-02' AS reporting_date, 2 AS a_id, 20 AS b_id, 150 AS amount
      )
    joins:
      - name: beta_dims
        relationship: many_to_one
        sql: "{CUBE}.a_id = {beta_dims.a_id}"
      - name: gamma_dims
        relationship: many_to_one
        sql: "{CUBE}.b_id = {gamma_dims.b_id}"
      - name: delta_bridge
        relationship: many_to_one
        sql: "{beta_dims.a_name} = {delta_bridge.a_name} AND {gamma_dims.b_name} = {delta_bridge.b_name}"
    dimensions:
      - name: reporting_date
        sql: reporting_date
        type: time
        primary_key: true
      - name: a_id
        sql: a_id
        type: number
        primary_key: true
      - name: b_id
        sql: b_id
        type: number
        primary_key: true
      - name: channel
        sql: "{delta_bridge.channel}"
        type: string
    measures:
      - name: amount_sum
        sql: amount
        type: sum

  - name: beta_dims
    sql: |
      (
        SELECT 1 AS a_id, 'Alpha1' AS a_name
        UNION ALL
        SELECT 2 AS a_id, 'Alpha2' AS a_name
      )
    dimensions:
      - name: a_id
        sql: a_id
        type: number
        primary_key: true
      - name: a_name
        sql: a_name
        type: string

  - name: gamma_dims
    sql: |
      (
        SELECT 10 AS b_id, 'Beta1' AS b_name
        UNION ALL
        SELECT 20 AS b_id, 'Beta2' AS b_name
      )
    dimensions:
      - name: b_id
        sql: b_id
        type: number
        primary_key: true
      - name: b_name
        sql: b_name
        type: string

  - name: delta_bridge
    sql: |
      (
        SELECT 'Alpha1' AS a_name, 'Beta1' AS b_name, 'Organic' AS channel
        UNION ALL
        SELECT 'Alpha1' AS a_name, 'Beta2' AS b_name, 'Paid' AS channel
        UNION ALL
        SELECT 'Alpha2' AS a_name, 'Beta1' AS b_name, 'Referral' AS channel
      )
    joins:
      - name: gamma_dims
        relationship: many_to_one
        sql: "{CUBE}.b_name = {gamma_dims.b_name}"
    dimensions:
      - name: a_name
        sql: a_name
        type: string
        primary_key: true
      - name: b_name
        sql: "{gamma_dims.b_name}"
        type: string
        primary_key: true
      - name: channel
        sql: channel
        type: string
      `);

    if (!getEnv('nativeSqlPlanner')) {
      it('querying cube dimension that require transitive joins', async () => {
        await compiler.compile();
        const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
          measures: [],
          dimensions: [
            'test_facts.reporting_date',
            'test_facts.merchant_sk',
            'test_facts.product_sk',
            'test_facts.acquisition_channel'
          ],
          order: [{
            id: 'test_facts.acquisition_channel'
          }],
          timezone: 'America/Los_Angeles'
        });

        const res = await dbRunner.testQuery(query.buildSqlAndParams());
        console.log(JSON.stringify(res));

        expect(res).toEqual([
          {
            test_facts__acquisition_channel: 'Organic',
            test_facts__merchant_sk: 101,
            test_facts__product_sk: 201,
            test_facts__reporting_date: '2023-01-01T00:00:00.000Z',
          },
          {
            test_facts__acquisition_channel: 'Paid',
            test_facts__merchant_sk: 101,
            test_facts__product_sk: 202,
            test_facts__reporting_date: '2023-01-01T00:00:00.000Z',
          },
          {
            test_facts__acquisition_channel: 'Referral',
            test_facts__merchant_sk: 102,
            test_facts__product_sk: 201,
            test_facts__reporting_date: '2023-01-02T00:00:00.000Z',
          },
        ]);
      });
    } else {
      it.skip('FIXME(tesseract): querying cube dimension that require transitive joins', async () => {
        // FIXME should be implemented in Tesseract
      });
    }

    if (!getEnv('nativeSqlPlanner')) {
      it('querying cube with transitive joins with loop', async () => {
        await compiler.compile();

        try {
          const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
            measures: [],
            dimensions: [
              'alpha_facts.reporting_date',
              'delta_bridge.b_name',
              'alpha_facts.channel'
            ],
            order: [{
              id: 'alpha_facts.reporting_date'
            }],
            timezone: 'America/Los_Angeles'
          });

          await dbRunner.testQuery(query.buildSqlAndParams());
          throw new Error('Should have thrown an error');
        } catch (err: any) {
          expect(err.message).toContain('Can not construct joins for the query, potential loop detected');
        }
      });
    } else {
      it.skip('FIXME(tesseract): querying cube dimension that require transitive joins', async () => {
        // FIXME should be implemented in Tesseract
      });
    }
  });
});
