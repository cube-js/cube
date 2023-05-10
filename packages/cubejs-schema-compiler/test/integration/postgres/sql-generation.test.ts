import { UserError } from '../../../src/compiler/UserError';
import { PostgresQuery } from '../../../src/adapter/PostgresQuery';
import { BigqueryQuery } from '../../../src/adapter/BigqueryQuery';
import { PrestodbQuery } from '../../../src/adapter/PrestodbQuery';
import { prepareCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';
import { createJoinedCubesSchema } from '../../unit/utils';

describe('SQL Generation', () => {
  jest.setTimeout(200000);

  const { compiler, joinGraph, cubeEvaluator } = prepareCompiler(`
    const perVisitorRevenueMeasure = {
      type: 'number',
      sql: new Function('visitor_revenue', 'visitor_count', 'return visitor_revenue + "/" + visitor_count')
    }
  
    cube(\`visitors\`, {
      sql: \`
      select * from visitors WHERE \${USER_CONTEXT.source.filter('source')} AND
      \${USER_CONTEXT.sourceArray.filter(sourceArray => \`source in (\${sourceArray.join(',')})\`)}
      \`,
      
      rewriteQueries: true,
      
      refreshKey: {
        sql: 'SELECT 1',
      },

      joins: {
        visitor_checkins: {
          relationship: 'hasMany',
          sql: \`\${CUBE}.id = \${visitor_checkins}.visitor_id\`
        }
      },

      measures: {
        visitor_count: {
          type: 'number',
          sql: \`count(*)\`,
          aliases: ['users count']
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
        revenueRolling3day: {
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
        ...(['foo', 'bar'].map(m => ({ [m]: { type: 'count' } })).reduce((a, b) => ({ ...a, ...b })))
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
        created_at: {
          type: 'time',
          sql: 'created_at'
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
          sql: \`replace('some string question string???', 'string', 'with some ???')\`,
          type: \`string\`
        }
      }
    })

    cube('visitor_checkins', {
      sql: \`
      select * from visitor_checkins WHERE \${FILTER_PARAMS.visitor_checkins.created_at.filter('created_at')}
      \`,
      
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
    })

    cube('visitor_checkins_sources', {
      sql: \`
      select id, source from visitor_checkins WHERE \${FILTER_PARAMS.visitor_checkins_sources.source.filter('source')}
      \`,

      rewriteQueries: true,

      joins: {
        cards: {
          relationship: 'hasMany',
          sql: \`\${CUBE}.id = \${cards}.visitor_checkin_id\`
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

    const queryAndParams = query.buildSqlAndParams();
    console.log(queryAndParams);

    return dbRunner.testQuery(query.buildSqlAndParams()).then(res => {
      expect(res).toEqual(
        [
          {
            visitors__created_at_day: '2017-01-02T00:00:00.000Z',
            visitors__visitor_revenue: '100',
            visitors__visitor_count: '1',
            visitor_checkins__visitor_checkins_count: '3',
            visitors__per_visitor_revenue: '100'
          },
          {
            visitors__created_at_day: '2017-01-04T00:00:00.000Z',
            visitors__visitor_revenue: '200',
            visitors__visitor_count: '1',
            visitor_checkins__visitor_checkins_count: '2',
            visitors__per_visitor_revenue: '200'
          },
          {
            visitors__created_at_day: '2017-01-05T00:00:00.000Z',
            visitors__visitor_revenue: null,
            visitors__visitor_count: '1',
            visitor_checkins__visitor_checkins_count: '1',
            visitors__per_visitor_revenue: null
          },
          {
            visitors__created_at_day: '2017-01-06T00:00:00.000Z',
            visitors__visitor_revenue: null,
            visitors__visitor_count: '2',
            visitor_checkins__visitor_checkins_count: '0',
            visitors__per_visitor_revenue: null
          }
        ]
      );
    });
  });

  async function runQueryTest(q, expectedResult) {
    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, q);

    console.log(query.buildSqlAndParams());

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
    visitor_checkins__visitor_checkins_count: '6',
    visitors__per_visitor_revenue: '60'
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
  }, [
    {
      visitors__created_at_day: '2017-01-02T00:00:00.000Z',
      visitors__revenue_rolling: null,
      visitor_checkins__visitor_checkins_count: '3'
    },
    {
      visitors__created_at_day: '2017-01-04T00:00:00.000Z',
      visitors__revenue_rolling: '100',
      visitor_checkins__visitor_checkins_count: '2'
    },
    {
      visitors__created_at_day: '2017-01-05T00:00:00.000Z',
      visitors__revenue_rolling: '200',
      visitor_checkins__visitor_checkins_count: '1'
    },
    {
      visitors__created_at_day: '2017-01-06T00:00:00.000Z',
      visitors__revenue_rolling: '500',
      visitor_checkins__visitor_checkins_count: '0'
    }
  ]));

  it('rolling month', async () => runQueryTest({
    measures: [
      'visitors.revenueRolling3day'
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
    { visitors__created_at_week: '2017-01-09T00:00:00.000Z', visitors__revenue_rolling3day: '900' }
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

    console.log(query.buildSqlAndParams());

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
    });

    console.log(query.buildSqlAndParams());

    expect(query.buildSqlAndParams()[0]).toMatch(/OFFSET (\d) LIMIT (\d)/);
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
        [{ visitor_checkins__revenue_per_checkin: '50' }]
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
        [{ visitor_checkins__google_sourced_checkins: '1' }]
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
        [{ visitor_checkins__google_sourced_checkins: '1' }]
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
      visitor_checkins__cards_count: '0',
      visitors__visitor_revenue: '300'
    },
    {
      visitor_checkins__cards_count: '1',
      visitors__visitor_revenue: '100'
    },
    {
      visitor_checkins__cards_count: null,
      visitors__visitor_revenue: null
    }
  ]));

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

    return dbRunner.testQueries(preAggregationsDescription.invalidateKeyQueries.concat([
      [preAggregationsDescription.loadSql[0].replace('CREATE TABLE', 'CREATE TEMP TABLE'), preAggregationsDescription.loadSql[1]],
      query.buildSqlAndParams()
    ])).then(res => {
      console.log(JSON.stringify(res));
      expect(res).toEqual(
        [
          {
            visitor_checkins__source: 'google',
            visitors__created_at_day: '2017-01-02T00:00:00.000Z',
            visitors__per_visitor_revenue: '100'
          }
        ]
      );
    });
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

    return dbRunner.testQueries(preAggregationsDescription.invalidateKeyQueries.concat([
      [
        preAggregationsDescription.loadSql[0].replace('CREATE TABLE', 'CREATE TEMP TABLE'),
        preAggregationsDescription.loadSql[1]
      ],
      query.buildSqlAndParams()
    ])).then(res => {
      console.log(JSON.stringify(res));
      expect(res).toEqual(
        [{
          visitor_checkins__source: 'google',
          visitors__created_at_day: '2017-01-02T00:00:00.000Z',
          visitors__visitor_revenue: '100'
        }]
      );
    });
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
        [{ visitor_checkins__revenue_per_checkin: '60' }]
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
        [{ visitor_checkins__revenue_per_checkin: '50' }]
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
      from: '2017-01-01T00:00:00.000',
      to: '2017-01-10T23:59:59.999'
    },
    {
      granularity: 'week',
      from: '2016-12-26T00:00:00.000',
      to: '2017-01-15T23:59:59.999'
    },
    {
      granularity: 'month',
      from: '2017-01-01T00:00:00.000',
      to: '2017-01-31T23:59:59.999'
    },
    {
      granularity: 'year',
      from: '2017-01-01T00:00:00.000',
      to: '2017-12-31T23:59:59.999'
    }
  ];

  // eslint-disable-next-line
  for (const granularityTest of granularityCases) {
    // eslint-disable-next-line no-loop-func
    it(`Should date with TZ, when pass timeDimensions with granularity by ${granularityTest.granularity}`, async () => {
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

  it('columns order for the query with the sub-query', async () => {
    const joinedSchemaCompilers = prepareCompiler(createJoinedCubesSchema());
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
});
