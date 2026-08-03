import { PostgresQuery } from '../../../src/adapter/PostgresQuery';
import { prepareJsCompiler, prepareCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

describe('PreAggregationsCalulatedMeasures', () => {
  jest.setTimeout(200000);

  const { compiler, joinGraph, cubeEvaluator } = prepareJsCompiler(`
  cube(\`visitors\`, {
    sql: \`
    select * from visitors WHERE \${FILTER_PARAMS.visitors.createdAt.filter('created_at')}
    \`,
    sqlAlias: 'vis',

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
      revenue: {
        sql: 'amount',
        type: 'sum'
      },


      average: {
        sql: \`\${revenue} / \${count}\`,
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
        sql: 'created_at',
      },
      checkinsCount: {
        type: 'number',
        sql: \`\${visitor_checkins.count}\`,
        subQuery: true,
        propagateFiltersToSubQuery: true
      },
      revTest: {
        sql: \`CONCAT(\${source},  \${createdAtDay})\`,
        type: 'string',
      },

      createdAtDay: {
        type: 'time',
        sql: \`\${createdAt.day}\`,
      },



    },

    segments: {
      google: {
        sql: \`source = 'google'\`
      }
    },

    preAggregations: {
        averagePreAgg: {
            type: 'rollup',
            measureReferences: [visitor_checkins.average, visitor_checkins.revenue, visitor_checkins.count],
            dimensionReferences: [source, id],
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
      },
      revenue: {
        sql: 'id',
        type: 'sum'
      },
      average: {
        sql: \`\${revenue} / \${count}\`,
        type: 'number'
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
        sql: 'created_at',
      }
    },

  })

  cube('facts', {
    sql: 'select * from visitor_checkins',
    sqlAlias: 'f',
    measures: {
      count: { type: 'count' },
      total_cost: { sql: 'id', type: 'sum' },
      avg_cost: { sql: \`\${CUBE.total_cost} / \${CUBE.count}\`, type: 'number' },
    },
    dimensions: {
      id: { type: 'number', sql: 'id', primaryKey: true },
      line_item_id: { type: 'number', sql: 'visitor_id' },
      day: { type: 'time', sql: 'created_at' },
    },
    preAggregations: {
      facts_rollup: {
        type: 'rollup',
        measures: [CUBE.count, CUBE.total_cost, CUBE.avg_cost],
        dimensions: [CUBE.line_item_id],
        timeDimension: CUBE.day,
        granularity: 'day',
      }
    }
  })

  cube('line_items', {
    sql: 'select * from visitors',
    sqlAlias: 'li',
    joins: {
      facts: {
        relationship: 'one_to_many',
        sql: \`\${CUBE.id} = \${facts.line_item_id}\`
      },
      campaigns: {
        relationship: 'many_to_one',
        sql: \`\${CUBE.id} = \${campaigns.id}\`
      }
    },
    measures: {
      count: { type: 'count' }
    },
    dimensions: {
      id: { type: 'number', sql: 'id', primaryKey: true },
      name: { type: 'string', sql: 'source' },
    },
    preAggregations: {
      li_rollup: {
        type: 'rollup',
        dimensions: [CUBE.id, CUBE.name],
      },
      combined_rollup_join: {
        type: 'rollupJoin',
        measures: [line_items.facts.count, line_items.facts.total_cost, line_items.facts.avg_cost],
        dimensions: [CUBE.name, campaigns.campaign_name],
        timeDimension: line_items.facts.day,
        granularity: 'day',
        rollups: [campaigns.campaigns_rollup, facts.facts_rollup, CUBE.li_rollup],
      }
    }
  })

  cube('campaigns', {
    sql: "select 1 as id, 'camp1' as campaign_name",
    sqlAlias: 'c',
    measures: {
      count: { type: 'count' }
    },
    dimensions: {
      id: { type: 'number', sql: 'id', primaryKey: true },
      campaign_name: { type: 'string', sql: 'campaign_name' },
    },
    preAggregations: {
      campaigns_rollup: {
        type: 'rollup',
        dimensions: [CUBE.id, CUBE.campaign_name],
      }
    }
  })

  view('my_view', {
    cubes: [
      { join_path: line_items.facts, includes: '*', prefix: true },
      { join_path: line_items, includes: '*', prefix: true },
      { join_path: line_items.campaigns, includes: '*', prefix: true },
    ]
  })

   `);

  it('rollupJoin matching with additive measures through view', async () => {
    await compiler.compile();

    const query = new PostgresQuery(
      { joinGraph, cubeEvaluator, compiler },
      {
        measures: [
          'my_view.facts_count',
          'my_view.facts_total_cost',
        ],
        timeDimensions: [{
          dimension: 'my_view.facts_day',
          granularity: 'day',
        }],
        timezone: 'America/Los_Angeles',
        preAggregationsSchema: '',
      }
    );

    const matchedPreAgg = query.preAggregations?.findPreAggregationForQuery();

    const sqlAndParams = query.buildSqlAndParams();
    expect(sqlAndParams[0]).toContain('campaigns_rollup');
    expect(sqlAndParams[0]).toContain('facts_rollup');
    expect(sqlAndParams[0]).toContain('li_rollup');
    expect(matchedPreAgg).toBeDefined();
    expect(matchedPreAgg?.preAggregationName).toEqual('combined_rollup_join');
    return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
      expect(res).toEqual(

        [
          {
            my_view__facts_day_day: '2017-01-02T00:00:00.000Z',
            my_view__facts_count: '1',
            my_view__facts_total_cost: '1'
          },
          {
            my_view__facts_day_day: '2017-01-03T00:00:00.000Z',
            my_view__facts_count: '1',
            my_view__facts_total_cost: '2'
          },
          {
            my_view__facts_day_day: '2017-01-04T00:00:00.000Z',
            my_view__facts_count: '3',
            my_view__facts_total_cost: '12'
          },
          {
            my_view__facts_day_day: '2017-01-05T00:00:00.000Z',
            my_view__facts_count: '1',
            my_view__facts_total_cost: '6'
          },
          {
            my_view__facts_day_day: null,
            my_view__facts_count: null,
            my_view__facts_total_cost: null
          }
        ]

      );
    });
  });

  it('rollupJoin matching with additive measures', async () => {
    await compiler.compile();

    const query = new PostgresQuery(
      { joinGraph, cubeEvaluator, compiler },
      {
        measures: [
          'facts.count',
          'facts.total_cost',
        ],
        dimensions: ['line_items.name'],
        timezone: 'America/Los_Angeles',
        preAggregationsSchema: '',
      }
    );

    const matchedPreAgg = query.preAggregations?.findPreAggregationForQuery();

    const sqlAndParams = query.buildSqlAndParams();
    expect(sqlAndParams[0]).toContain('campaigns_rollup');
    expect(sqlAndParams[0]).toContain('facts_rollup');
    expect(sqlAndParams[0]).toContain('li_rollup');
    expect(matchedPreAgg).toBeDefined();
    expect(matchedPreAgg?.preAggregationName).toEqual('combined_rollup_join');
    return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
      expect(res).toEqual(

        [
          { li__name: null, f__count: null, f__total_cost: null },
          { li__name: 'some', f__count: '5', f__total_cost: '15' },
          { li__name: 'google', f__count: '1', f__total_cost: '6' }
        ]

      );
    });
  });

  it('rollupJoin matching with calculated measures through view', async () => {
    await compiler.compile();

    const query = new PostgresQuery(
      { joinGraph, cubeEvaluator, compiler },
      {
        measures: [
          'my_view.facts_avg_cost',
        ],
        timeDimensions: [{
          dimension: 'my_view.facts_day',
          granularity: 'day',
        }],
        timezone: 'America/Los_Angeles',
        preAggregationsSchema: '',
      }
    );

    const matchedPreAgg = query.preAggregations?.findPreAggregationForQuery();

    const sqlAndParams = query.buildSqlAndParams();
    expect(sqlAndParams[0]).toContain('campaigns_rollup');
    expect(sqlAndParams[0]).toContain('facts_rollup');
    expect(sqlAndParams[0]).toContain('li_rollup');
    expect(matchedPreAgg).toBeDefined();
    expect(matchedPreAgg?.preAggregationName).toEqual('combined_rollup_join');
    return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
      expect(res).toEqual(

        [
          {
            my_view__facts_day_day: '2017-01-02T00:00:00.000Z',
            my_view__facts_avg_cost: '1.00000000000000000000'
          },
          {
            my_view__facts_day_day: '2017-01-03T00:00:00.000Z',
            my_view__facts_avg_cost: '2.0000000000000000'
          },
          {
            my_view__facts_day_day: '2017-01-04T00:00:00.000Z',
            my_view__facts_avg_cost: '4.0000000000000000'
          },
          {
            my_view__facts_day_day: '2017-01-05T00:00:00.000Z',
            my_view__facts_avg_cost: '6.0000000000000000'
          },
          { my_view__facts_day_day: null, my_view__facts_avg_cost: null }
        ]

      );
    });
  });

  it('calculated measure pre-aggregation', () => compiler.compile().then(() => {
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitor_checkins.average',
        'visitor_checkins.revenue',
        'visitor_checkins.count'
      ],
      dimensions: [
        'visitors.source'
      ],
      timezone: 'America/Los_Angeles',
      preAggregationsSchema: '',
      cubestoreSupportMultistage: true
    });

    const preAggregationsDescription: any = query.preAggregations?.preAggregationsDescription();
    const sqlAndParams = query.buildSqlAndParams();
    expect(preAggregationsDescription[0].tableName).toEqual('vis_average_pre_agg');
    expect(sqlAndParams[0]).toContain('vis_average_pre_agg');

    return dbRunner.evaluateQueryWithPreAggregations(query).then(res => {
      expect(res).toEqual(

        [
          {
            vis__source: null,
            vc__average: null,
            vc__revenue: null,
            vc__count: '0'
          },
          {
            vis__source: 'google',
            vc__average: '6.0000000000000000',
            vc__revenue: '6',
            vc__count: '1'
          },
          {
            vis__source: 'some',
            vc__average: '3.0000000000000000',
            vc__revenue: '15',
            vc__count: '5'
          }
        ]

      );
    });
  }));
});
