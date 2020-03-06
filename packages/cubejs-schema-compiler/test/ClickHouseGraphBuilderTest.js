/* eslint-disable quote-props */
/* globals describe, after, it */
const UserError = require('../compiler/UserError');
const PrepareCompiler = require('./PrepareCompiler');
require('should');

const { prepareCompiler } = PrepareCompiler;
const dbRunner = require('./ClickHouseDbRunner');

const { debugLog, logSqlAndParams } = require('./TestUtil');

describe('ClickHouse JoinGraph', function test() {
  this.timeout(20000);

  after(async () => {
    await dbRunner.tearDown();
  });

  const { compiler, joinGraph, cubeEvaluator, transformer } = prepareCompiler(`
    const perVisitorRevenueMeasure = {
      type: 'number',
      sql: new Function('visitor_revenue', 'visitor_count', 'return visitor_revenue + "/" + visitor_count')
    }
  
    cube(\`visitors\`, {
      sql: \`
      select * from visitors WHERE \${USER_CONTEXT.source.filter('source')} AND
      \${USER_CONTEXT.sourceArray.filter(sourceArray => \`source in (\${sourceArray.join(',')})\`)}
      \`,
      
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
        }
      }
    })

    cube('visitor_checkins', {
      sql: \`
      select * from visitor_checkins
      \`,

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
      
      // preAggregations: {
      //   checkinSource: {
      //     type: 'rollup',
      //     measureReferences: [visitors.per_visitor_revenue],
      //     dimensionReferences: [visitor_checkins.source],
      //     timeDimensionReference: visitors.created_at,
      //     granularity: 'day'
      //   },
      //   visitorCountCheckinSource: {
      //     type: 'rollup',
      //     measureReferences: [visitors.visitor_revenue],
      //     dimensionReferences: [visitor_checkins.source],
      //     timeDimensionReference: visitors.created_at,
      //     granularity: 'day'
      //   }
      // }
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
        WHERE \${FILTER_PARAMS.ReferenceVisitors.createdAt.filter(\`addDays(t.created_at, 28)\`)} AND
        \${FILTER_PARAMS.ReferenceVisitors.createdAt.filter((from, to) => \`(addDays(t.created_at,28)) >= parseDateTimeBestEffort(\${from}) AND (addDays(t.created_at, 28)) <= parseDateTimeBestEffort(\${to})\`)}
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
    `);

  // FAILS - ClickHouse doesn't support OR in JOIN expressions
  it.skip('simple join', () => {
    const result = compiler.compile().then(() => {
      debugLog(joinGraph.buildJoin(['visitor_checkins', 'visitors']));

      const query = dbRunner.newQuery({ joinGraph, cubeEvaluator, compiler }, {
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

      logSqlAndParams(query)

      return dbRunner.testQuery(query.buildSqlAndParams()).then(res => {
        res.should.be.deepEqual(
          [
            {
              "visitors__created_at_day": "2017-01-02T00:00:00.000Z",
              "visitors__visitor_revenue": "100",
              "visitors__visitor_count": "1",
              "visitor_checkins__visitor_checkins_count": "3",
              "visitors__per_visitor_revenue": "100"
            },
            {
              "visitors__created_at_day": "2017-01-04T00:00:00.000Z",
              "visitors__visitor_revenue": "200",
              "visitors__visitor_count": "1",
              "visitor_checkins__visitor_checkins_count": "2",
              "visitors__per_visitor_revenue": "200"
            },
            {
              "visitors__created_at_day": "2017-01-05T00:00:00.000Z",
              "visitors__visitor_revenue": null,
              "visitors__visitor_count": "1",
              "visitor_checkins__visitor_checkins_count": "1",
              "visitors__per_visitor_revenue": null
            },
            {
              "visitors__created_at_day": "2017-01-06T00:00:00.000Z",
              "visitors__visitor_revenue": null,
              "visitors__visitor_count": "2",
              "visitor_checkins__visitor_checkins_count": "0",
              "visitors__per_visitor_revenue": null
            }
          ]
        );
      });
    });

    return result;
  });

  it('simple join total', () =>
      runQueryTest({
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
        "visitors__visitor_revenue": "300",
        "visitors__visitor_count": "5",
        "visitor_checkins__visitor_checkins_count": "6",
        "visitors__per_visitor_revenue": "60"
      }])
  );

  // FAILS - need to finish query to override ::timestamptz
  it.skip('running total', () => {
    const result = compiler.compile().then(() => {
      let query = dbRunner.newQuery({ joinGraph, cubeEvaluator, compiler }, {
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

      logSqlAndParams(query)

      // TODO ordering doesn't work for running total
      return dbRunner.testQuery(query.buildSqlAndParams()).then(res => {
        debugLog(JSON.stringify(res));
        res.should.be.deepEqual(
          [{
            "visitors__created_at_day": "2017-01-01T00:00:00.000Z",
            "visitors__revenue_running": null
          }, {
            "visitors__created_at_day": "2017-01-02T00:00:00.000Z",
            "visitors__revenue_running": "100"
          }, {
            "visitors__created_at_day": "2017-01-03T00:00:00.000Z",
            "visitors__revenue_running": "100"
          }, {
            "visitors__created_at_day": "2017-01-04T00:00:00.000Z",
            "visitors__revenue_running": "300"
          }, {
            "visitors__created_at_day": "2017-01-05T00:00:00.000Z",
            "visitors__revenue_running": "600"
          }, {
            "visitors__created_at_day": "2017-01-06T00:00:00.000Z",
            "visitors__revenue_running": "1500"
          }, {
            "visitors__created_at_day": "2017-01-07T00:00:00.000Z",
            "visitors__revenue_running": "1500"
          }, {
            "visitors__created_at_day": "2017-01-08T00:00:00.000Z",
            "visitors__revenue_running": "1500"
          }, {
            "visitors__created_at_day": "2017-01-09T00:00:00.000Z",
            "visitors__revenue_running": "1500"
          }, {
            "visitors__created_at_day": "2017-01-10T00:00:00.000Z",
            "visitors__revenue_running": "1500"
          }]
        );
      });
    });

    return result;
  });

  // FAILS - need to finish query to override ::timestamptz
  it.skip('rolling', () =>
      runQueryTest({
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
        { "visitors__created_at_day": "2017-01-01T00:00:00.000Z", "visitors__revenue_rolling": null },
        { "visitors__created_at_day": "2017-01-02T00:00:00.000Z", "visitors__revenue_rolling": null },
        { "visitors__created_at_day": "2017-01-03T00:00:00.000Z", "visitors__revenue_rolling": "100" },
        { "visitors__created_at_day": "2017-01-04T00:00:00.000Z", "visitors__revenue_rolling": "100" },
        { "visitors__created_at_day": "2017-01-05T00:00:00.000Z", "visitors__revenue_rolling": "200" },
        { "visitors__created_at_day": "2017-01-06T00:00:00.000Z", "visitors__revenue_rolling": "500" },
        { "visitors__created_at_day": "2017-01-07T00:00:00.000Z", "visitors__revenue_rolling": "1200" },
        { "visitors__created_at_day": "2017-01-08T00:00:00.000Z", "visitors__revenue_rolling": "900" },
        { "visitors__created_at_day": "2017-01-09T00:00:00.000Z", "visitors__revenue_rolling": null },
        { "visitors__created_at_day": "2017-01-10T00:00:00.000Z", "visitors__revenue_rolling": null }
      ])
  );

  // FAILS - need to finish query to override ::timestamptz
  it.skip('rolling multiplied', () =>
    runQueryTest({
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
        "visitors__created_at_day": "2017-01-02T00:00:00.000Z", "visitors__revenue_rolling": null,
        "visitor_checkins__visitor_checkins_count": "3"
      },
      {
        "visitors__created_at_day": "2017-01-04T00:00:00.000Z", "visitors__revenue_rolling": "100",
        'visitor_checkins.visitor_checkins_count': '2'
      },
      {
        "visitors__created_at_day": "2017-01-05T00:00:00.000Z", "visitors__revenue_rolling": "200",
        'visitor_checkins.visitor_checkins_count': '1'
      },
      {
        "visitors__created_at_day": "2017-01-06T00:00:00.000Z", "visitors__revenue_rolling": "500",
        'visitor_checkins.visitor_checkins_count': '0'
      }
    ])
  );

  // FAILS - Syntax error: failed at position 107
  it.skip('rolling month', () =>
    runQueryTest({
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
      { "visitors__created_at_week": "2017-01-09T00:00:00.000Z", "visitors__revenue_rolling3day": "900" }
    ])
  );

  // FAILS - Syntax error: failed at position 249
  it.skip('rolling count', () =>
    runQueryTest({
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
      { "visitors__created_at_day": "2017-01-01T00:00:00.000Z", "visitors__count_rolling": null },
      { "visitors__created_at_day": "2017-01-02T00:00:00.000Z", "visitors__count_rolling": null },
      { "visitors__created_at_day": "2017-01-03T00:00:00.000Z", "visitors__count_rolling": "1" },
      { "visitors__created_at_day": "2017-01-04T00:00:00.000Z", "visitors__count_rolling": "1" },
      { "visitors__created_at_day": "2017-01-05T00:00:00.000Z", "visitors__count_rolling": "1" },
      { "visitors__created_at_day": "2017-01-06T00:00:00.000Z", "visitors__count_rolling": "2" },
      { "visitors__created_at_day": "2017-01-07T00:00:00.000Z", "visitors__count_rolling": "3" },
      { "visitors__created_at_day": "2017-01-08T00:00:00.000Z", "visitors__count_rolling": "2" },
      { "visitors__created_at_day": "2017-01-09T00:00:00.000Z", "visitors__count_rolling": null },
      { "visitors__created_at_day": "2017-01-10T00:00:00.000Z", "visitors__count_rolling": null }
    ])
  );

  it('sql utils', () =>
    runQueryTest({
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
      {"visitors__created_at_sql_utils_day":"2017-01-02T00:00:00.000","visitors__visitor_count":"1"},
      {"visitors__created_at_sql_utils_day":"2017-01-04T00:00:00.000","visitors__visitor_count":"1"},
      {"visitors__created_at_sql_utils_day":"2017-01-05T00:00:00.000","visitors__visitor_count":"1"},
      {"visitors__created_at_sql_utils_day":"2017-01-06T00:00:00.000","visitors__visitor_count":"2"}
    ])
  );

  it('running total total', () =>
      runQueryTest({
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
          "visitors__revenue_running": "1500"
        }
      ])
  );

  // FAILS Unmatched parentheses
  it.skip('running total ratio', () =>
      runQueryTest({
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
        { "visitors__created_at_day": "2017-01-01T00:00:00.000Z", "visitors__running_revenue_per_count": null },
        { "visitors__created_at_day": "2017-01-02T00:00:00.000Z", "visitors__running_revenue_per_count": "100" },
        { "visitors__created_at_day": "2017-01-03T00:00:00.000Z", "visitors__running_revenue_per_count": "100" },
        { "visitors__created_at_day": "2017-01-04T00:00:00.000Z", "visitors__running_revenue_per_count": "150" },
        { "visitors__created_at_day": "2017-01-05T00:00:00.000Z", "visitors__running_revenue_per_count": "200" },
        { "visitors__created_at_day": "2017-01-06T00:00:00.000Z", "visitors__running_revenue_per_count": "300" },
        { "visitors__created_at_day": "2017-01-07T00:00:00.000Z", "visitors__running_revenue_per_count": "300" },
        { "visitors__created_at_day": "2017-01-08T00:00:00.000Z", "visitors__running_revenue_per_count": "300" },
        { "visitors__created_at_day": "2017-01-09T00:00:00.000Z", "visitors__running_revenue_per_count": "300" },
        { "visitors__created_at_day": "2017-01-10T00:00:00.000Z", "visitors__running_revenue_per_count": "300" }
      ])
  );

  // FAILS ClickHouse supports multiple approximate aggregators : uniq, uniqCombined, uniqHLL12, need to pick one to use and implement it in query
  it.skip('hll rolling', () => {
    const result = compiler.compile().then(() => {
      let query = dbRunner.newQuery({ joinGraph, cubeEvaluator, compiler }, {
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

      logSqlAndParams(query)

      query.buildSqlAndParams()[0].should.match(/HLL_COUNT\.MERGE/);
      query.buildSqlAndParams()[0].should.match(/HLL_COUNT\.INIT/);
    });

    return result;
  });

  it('calculated join', () => {
    const result = compiler.compile().then(() => {
      let query = dbRunner.newQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: [
          'visitor_checkins.revenue_per_checkin'
        ],
        timeDimensions: [],
        timezone: 'America/Los_Angeles'
      });

      logSqlAndParams(query)

      return dbRunner.testQuery(query.buildSqlAndParams()).then(res => {
        debugLog(JSON.stringify(res));
        res.should.be.deepEqual(
          [{ "visitor_checkins__revenue_per_checkin": "50" }]
        );
      });
    });

    return result;
  });

  it('filter join', () => {
    const result = compiler.compile().then(() => {
      let query = dbRunner.newQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: [
          'visitor_checkins.google_sourced_checkins'
        ],
        timeDimensions: [],
        timezone: 'America/Los_Angeles'
      });

      logSqlAndParams(query)

      return dbRunner.testQuery(query.buildSqlAndParams()).then(res => {
        debugLog(JSON.stringify(res));
        res.should.be.deepEqual(
          [{ "visitor_checkins__google_sourced_checkins": "1" }]
        );
      });
    });

    return result;
  });

  it('filter join not multiplied', () => {
    const result = compiler.compile().then(() => {
      let query = dbRunner.newQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: [
          'visitor_checkins.google_sourced_checkins'
        ],
        timeDimensions: [],
        filters: [
          { dimension: 'cards.id', operator: 'equals', values: [3] }  // must be number
        ],
        timezone: 'America/Los_Angeles'
      });

      logSqlAndParams(query)

      return dbRunner.testQuery(query.buildSqlAndParams()).then(res => {
        debugLog(JSON.stringify(res));
        res.should.be.deepEqual(
          [{ "visitor_checkins__google_sourced_checkins": "1" }]
        );
      });
    });

    return result;
  });

  it('having filter', () => {
    return compiler.compile().then(() => {
      const query = dbRunner.newQuery({ joinGraph, cubeEvaluator, compiler }, {
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
          values: [1]  // must be a number
        }],
        order: [{
          id: 'visitors.source'
        }]
      });

      logSqlAndParams(query)
      
      return dbRunner.testQuery(query.buildSqlAndParams()).then(res => {
        debugLog(JSON.stringify(res));
        res.should.be.deepEqual(
          [{
            "visitors__source": "some",
            "visitors__visitor_count": "2"
          },{
            "visitors__source": null,
            "visitors__visitor_count": "3"
          }]
        );
      });
    });
  });

  it('having filter without measure', () =>
    compiler.compile().then(() => {
      const query = dbRunner.newQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: [],
        dimensions: [
          'visitors.source'
        ],
        timeDimensions: [],
        timezone: 'America/Los_Angeles',
        filters: [{
          dimension: 'visitors.visitor_count',
          operator: 'gt',
          values: [1] // must be a number
        }],
        order: [{
          id: 'visitors.source'
        }]
      });

      logSqlAndParams(query);

      return dbRunner.testQuery(query.buildSqlAndParams()).then(res => {
        debugLog(JSON.stringify(res));
        res.should.be.deepEqual(
          [          
            {
              "visitors__source": "some"
            },
            {
              "visitors__source": null
            },
          ]
        );
      });
    })
  );

  // FAILS - doesnt support OR in JOIN
  it.skip('having filter without measure with join', () =>
      compiler.compile().then(() => {
        const query = dbRunner.newQuery({ joinGraph, cubeEvaluator, compiler }, {
          measures: [],
          dimensions: [
            'visitors.source'
          ],
          timeDimensions: [],
          timezone: 'America/Los_Angeles',
          filters: [{
            dimension: 'visitor_checkins.revenue_per_checkin',
            operator: 'gte',
            values: [60]  // must be a number
          }],
          order: [{
            id: 'visitors.source'
          }]
        });

        logSqlAndParams(query)

        return dbRunner.testQuery(query.buildSqlAndParams()).then(res => {
          debugLog(JSON.stringify(res));
          res.should.be.deepEqual(
            [{
              "visitors__source": "some"
            }]
          );
        });
      })
  );

  it('having filter without measure single multiplied', () =>
      compiler.compile().then(() => {
        const query = dbRunner.newQuery({ joinGraph, cubeEvaluator, compiler }, {
          measures: [],
          dimensions: [
            'visitors.source'
          ],
          timeDimensions: [],
          timezone: 'America/Los_Angeles',
          filters: [{
            dimension: 'visitors.visitor_revenue',
            operator: 'gte',
            values: [1] // must be a number
          }, {
            dimension: 'visitor_checkins.source',
            operator: 'equals',
            values: ['google']
          }],
          order: [{
            id: 'visitors.source'
          }]
        });

        logSqlAndParams(query)

        return dbRunner.testQuery(query.buildSqlAndParams()).then(res => {
          debugLog(JSON.stringify(res));
          res.should.be.deepEqual(
            [{
              "visitors__source": "some"
            }]
          );
        });
      })
  );

  it('subquery', () => {
    const result = compiler.compile().then(() => {
      let query = dbRunner.newQuery({ joinGraph, cubeEvaluator, compiler }, {
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

      logSqlAndParams(query)

      return dbRunner.testQuery(query.buildSqlAndParams()).then(res => {
        debugLog(JSON.stringify(res));
        res.should.be.deepEqual(
          [{
            "visitors__checkins": "0",
            "visitors__created_at_day": "2017-01-06T00:00:00.000",
            "visitors__visitor_count": "2"
          }, {
            "visitors__checkins": "1",
            "visitors__created_at_day": "2017-01-05T00:00:00.000",
            "visitors__visitor_count": "1"
          }, {
            "visitors__checkins": "2",
            "visitors__created_at_day": "2017-01-04T00:00:00.000",
            "visitors__visitor_count": "1"
          }, {
            "visitors__checkins": "3",
            "visitors__created_at_day": "2017-01-02T00:00:00.000",
            "visitors__visitor_count": "1"
          }]
        );
      });
    });

    return result;
  });

  // ClickHouse does NOT support correlated subqueries
  // the SQL will have to be re-written to use array functions
  // FAILS Error: Unknown identifier: visitors.created_at_date
  it.skip('average subquery', () => {
    const result = compiler.compile().then(() => {
      let query = dbRunner.newQuery({ joinGraph, cubeEvaluator, compiler }, {
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

      logSqlAndParams(query)

      return dbRunner.testQuery(query.buildSqlAndParams()).then(res => {
        debugLog(JSON.stringify(res));
        res.should.be.deepEqual(
          [{ "visitors__created_at_day": "2017-01-02T00:00:00.000Z", "visitors__average_checkins": "6__0000000000000000" }]
        );
      });
    });

    return result;
  });

  function runQueryTest(q, expectedResult) {
    return compiler.compile().then(() => {
      const query = dbRunner.newQuery({ joinGraph, cubeEvaluator, compiler }, q);

      logSqlAndParams(query)

      return dbRunner.testQuery(query.buildSqlAndParams()).then(res => {
        debugLog(JSON.stringify(res));
        res.should.be.deepEqual(
          expectedResult
        );
      });
    });
  }

  it('subquery without measure', () =>
    runQueryTest({
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
        "visitors__min_visitor_checkin_date_day": "2017-01-02T00:00:00.000Z",
        "visitors__visitor_count": "1"
      },
      {
        "visitors__min_visitor_checkin_date_day": "2017-01-04T00:00:00.000Z",
        "visitors__visitor_count": "1"
      },
      {
        "visitors__min_visitor_checkin_date_day": "2017-01-05T00:00:00.000Z",
        "visitors__visitor_count": "1"
      }
    ]).then(() => {
      throw new Error();
    }).catch((error) => {
      error.should.be.instanceof(UserError);
    })
  );

  it('min date subquery', () =>
    runQueryTest({
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
        "visitors__min_visitor_checkin_date_day": "2017-01-02T00:00:00.000",
        "visitors__visitor_count": "1"
      },
      {
        "visitors__min_visitor_checkin_date_day": "2017-01-04T00:00:00.000",
        "visitors__visitor_count": "1"
      },
      {
        "visitors__min_visitor_checkin_date_day": "2017-01-05T00:00:00.000",
        "visitors__visitor_count": "1"
      }
    ])
  );

  it('min date subquery with error', () =>
    runQueryTest({
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
      error.should.be.instanceof(UserError);
    })
  );

  it('subquery dimension with join', () =>
      runQueryTest({
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
          "visitor_checkins__cards_count": "0",
          "visitors__visitor_revenue": "300"
        },
        {
          "visitor_checkins__cards_count": "1",
          "visitors__visitor_revenue": "100"
        },
        {
          "visitor_checkins__cards_count": null,
          "visitors__visitor_revenue": null
        }
      ])
  );

  // TODO
  it.skip('join rollup pre-aggregation', () => {
    return compiler.compile().then(() => {
      const query = dbRunner.newQuery({ joinGraph, cubeEvaluator, compiler }, {
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

      logSqlAndParams(query)

      const preAggregationsDescription = query.preAggregations.preAggregationsDescription()[0];
      debugLog(preAggregationsDescription);

      return dbRunner.testQueries(preAggregationsDescription.invalidateKeyQueries.concat([
        [preAggregationsDescription.loadSql[0].replace('CREATE TABLE', 'CREATE TEMP TABLE'), preAggregationsDescription.loadSql[1]],
        query.buildSqlAndParams()
      ])).then(res => {
        debugLog(JSON.stringify(res));
        res.should.be.deepEqual(
          [
            {
              "visitor_checkins__source": "google",
              "visitors__created_at_day": "2017-01-02T00:00:00.000Z",
              "visitors__per_visitor_revenue": "100"
            }
          ]
        );
      });
    });
  });

  // TODO
  it.skip('join rollup total pre-aggregation', () => {
    return compiler.compile().then(() => {
      const query = dbRunner.newQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: [
          'visitors.visitor_revenue'
        ],
        timeDimensions: [{
          dimension: 'visitors.created_at',
          dateRange: ['2017-01-01', '2017-01-30']
        }],
        timezone: 'America/Los_Angeles',
        order: [],
        filters: [{
          dimension: 'visitor_checkins.source',
          operator: 'equals',
          values: ['google']
        }],
        preAggregationsSchema: ''
      });

      logSqlAndParams(query)

      const preAggregationsDescription = query.preAggregations.preAggregationsDescription()[0];
      debugLog(preAggregationsDescription);

      return dbRunner.testQueries(preAggregationsDescription.invalidateKeyQueries.concat([
        [
          preAggregationsDescription.loadSql[0].replace('CREATE TABLE', 'CREATE TEMP TABLE'),
          preAggregationsDescription.loadSql[1]
        ],
        query.buildSqlAndParams()
      ])).then(res => {
        debugLog(JSON.stringify(res));
        res.should.be.deepEqual(
          [
            {
              "visitors__visitor_revenue": "100"
            }
          ]
        );
      });
    });
  });

  it('user context', () => {
    return compiler.compile().then(() => {
      let query = dbRunner.newQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: [
          'visitor_checkins.revenue_per_checkin'
        ],
        timeDimensions: [],
        timezone: 'America/Los_Angeles',
        contextSymbols: {
          userContext: { source: 'some' }
        }
      });

      logSqlAndParams(query)

      return dbRunner.testQuery(query.buildSqlAndParams()).then(res => {
        debugLog(JSON.stringify(res));
        res.should.be.deepEqual(
          [{ "visitor_checkins__revenue_per_checkin": "60" }]
        );
      });
    });
  });

  it('user context array', () => compiler.compile().then(() => {
    const query = dbRunner.newQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: [
        'visitor_checkins.revenue_per_checkin'
      ],
      timeDimensions: [],
      timezone: 'America/Los_Angeles',
      contextSymbols: {
        userContext: {
          sourceArray: ['some', 'google']
        }
      }
    });

    logSqlAndParams(query)

    return dbRunner.testQuery(query.buildSqlAndParams()).then(res => {
      debugLog(JSON.stringify(res));
      res.should.be.deepEqual(
        [{ "visitor_checkins__revenue_per_checkin": "50" }]
      );
    });
  }));

  it('reference cube sql', () =>
    runQueryTest({
      measures: [
        'ReferenceVisitors.count'
      ],
      timezone: 'America/Los_Angeles',
      order: [],
      timeDimensions: [{
        dimension: 'ReferenceVisitors.createdAt',
        dateRange: ['2017-01-01', '2017-01-30']
      }],
    }, [{ "reference_visitors__count": "1" }])
  );

  it('Filtered count without primaryKey', () =>
    runQueryTest({
      measures: [
        'ReferenceVisitors.googleSourcedCount'
      ],
      timezone: 'America/Los_Angeles',
      order: [],
      timeDimensions: [{
        dimension: 'ReferenceVisitors.createdAt',
        dateRange: ['2016-12-01', '2017-03-30']
      }],
    }, [{"reference_visitors__google_sourced_count":"1"}])
  );

  it('builds geo dimension', () =>
    runQueryTest({
      dimensions: [
        'visitors.location'
      ],
      timezone: 'America/Los_Angeles',
      order: [{ id: 'visitors.location' }],
    }, [
        // in ClickHouse float to string omits any trailing zeros after the decimal point
        { "visitors__location": '120.12,10.6' },
        { "visitors__location": '120.12,40.6' },
        { "visitors__location": '120.12,58.1' },
        { "visitors__location": '120.12,58.6' },
        { "visitors__location": '120.12,70.6' }
    ])
  );

  it('applies measure_filter type filter', () =>
    runQueryTest({
      dimensions: [
        'visitors.id'
      ],
      filters: [{
        dimension: 'visitors.visitor_revenue',
        operator: 'measure_filter'
      }],
      timezone: 'America/Los_Angeles',
      order: [{ id: 'visitors.id' }], // was visitors.location which is odd since its not in the select list
    }, [
      { "visitors__id": '1' }, // all numbers are transformed to strings.  ClickHouse returns large number types as strings so we normalise that to all numbers as strings
      { "visitors__id": '2' }
    ])
  );

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
      { "visitors__source": 'some' }
    ])
  );

  it('year granularity', () =>
    runQueryTest({
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
        "visitors__created_at_year": "2016-01-01T00:00:00.000",
        "visitors__visitor_count": "1"
      },
      {
        "visitors__created_at_year": "2017-01-01T00:00:00.000",
        "visitors__visitor_count": "5"
      }
    ])
  );
});
