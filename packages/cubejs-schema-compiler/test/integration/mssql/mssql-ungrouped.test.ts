import { MssqlQuery } from '../../../src/adapter/MssqlQuery';
import { prepareJsCompiler } from '../../unit/PrepareCompiler';
import { MSSqlDbRunner } from './MSSqlDbRunner';

describe('MSSqlUngrouped', () => {
  jest.setTimeout(200000);

  const dbRunner = new MSSqlDbRunner();

  afterAll(async () => {
    await dbRunner.tearDown();
  });

  const { compiler, joinGraph, cubeEvaluator } = prepareJsCompiler(`
    const perVisitorRevenueMeasure = {
      type: 'number',
      sql: new Function('visitor_revenue', 'visitor_count', 'return visitor_revenue + "/" + visitor_count')
    }

    cube(\`visitors\`, {
      sql: \`
      select * from ##visitors WHERE \${SECURITY_CONTEXT.source.filter('source')} AND
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
        strCase: {
          sql: \`CASE WHEN \${visitor_count} > 1 THEN 'More than 1' ELSE (\${visitor_revenue})::text END\`,
          type: \`string\`
        },
        unique_sources: {
          type: 'countDistinct',
          sql: \`source\`
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
          sql: \`replace('some string question string ? ?? ???', 'string', 'with some ? ?? ???')\`,
          type: \`string\`
        }
      }
    })

    cube('visitor_checkins', {
      sql: \`
      select * from ##visitor_checkins WHERE
      \${FILTER_PARAMS.visitor_checkins.created_at.filter('created_at')} AND
      \${FILTER_GROUP(FILTER_PARAMS.visitor_checkins.created_at.filter("dateadd(day, -3, created_at)"), FILTER_PARAMS.visitor_checkins.source.filter('source'))}
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
    })

    cube('visitor_checkins_sources', {
      sql: \`
      select id, source from ##visitor_checkins WHERE \${FILTER_PARAMS.visitor_checkins_sources.source.filter('source')}
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
      select * from ##cards
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



    `);

  async function runQueryTest(q, expectedResult) {
    await compiler.compile();
    const query = new MssqlQuery({ joinGraph, cubeEvaluator, compiler }, q);

    console.log(query.buildSqlAndParams());

    const res = await dbRunner.testQuery(query.buildSqlAndParams());
    console.log(JSON.stringify(res));

    expect(res).toEqual(
      expectedResult
    );
  }

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
    { visitor_checkins__created_at_day: new Date('2017-01-03T00:00:00.000Z'), visitor_checkins__google_sourced_checkins: null },
    { visitor_checkins__created_at_day: new Date('2017-01-04T00:00:00.000Z'), visitor_checkins__google_sourced_checkins: null },
    { visitor_checkins__created_at_day: new Date('2017-01-05T00:00:00.000Z'), visitor_checkins__google_sourced_checkins: null },
    { visitor_checkins__created_at_day: new Date('2017-01-05T00:00:00.000Z'), visitor_checkins__google_sourced_checkins: null },
    { visitor_checkins__created_at_day: new Date('2017-01-05T00:00:00.000Z'), visitor_checkins__google_sourced_checkins: null },
    { visitor_checkins__created_at_day: new Date('2017-01-06T00:00:00.000Z'), visitor_checkins__google_sourced_checkins: 1 },
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
    { visitor_checkins__created_at_day: new Date('2017-01-03T00:00:00.000Z'), visitor_checkins__unique_google_sourced_checkins: null },
    { visitor_checkins__created_at_day: new Date('2017-01-04T00:00:00.000Z'), visitor_checkins__unique_google_sourced_checkins: null },
    { visitor_checkins__created_at_day: new Date('2017-01-05T00:00:00.000Z'), visitor_checkins__unique_google_sourced_checkins: null },
    { visitor_checkins__created_at_day: new Date('2017-01-05T00:00:00.000Z'), visitor_checkins__unique_google_sourced_checkins: null },
    { visitor_checkins__created_at_day: new Date('2017-01-05T00:00:00.000Z'), visitor_checkins__unique_google_sourced_checkins: null },
    { visitor_checkins__created_at_day: new Date('2017-01-06T00:00:00.000Z'), visitor_checkins__unique_google_sourced_checkins: 1 },
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
    { visitor_checkins__created_at_day: new Date('2017-01-03T00:00:00.000Z'), visitor_checkins__unique_sources_per_checking: 1 },
    { visitor_checkins__created_at_day: new Date('2017-01-04T00:00:00.000Z'), visitor_checkins__unique_sources_per_checking: 1 },
    { visitor_checkins__created_at_day: new Date('2017-01-05T00:00:00.000Z'), visitor_checkins__unique_sources_per_checking: 1 },
    { visitor_checkins__created_at_day: new Date('2017-01-05T00:00:00.000Z'), visitor_checkins__unique_sources_per_checking: 1 },
    { visitor_checkins__created_at_day: new Date('2017-01-05T00:00:00.000Z'), visitor_checkins__unique_sources_per_checking: 1 },
    { visitor_checkins__created_at_day: new Date('2017-01-06T00:00:00.000Z'), visitor_checkins__unique_sources_per_checking: 1 },
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
    visitors__created_at_day: new Date('2016-09-07T00:00:00.000Z')
  }, {
    visitors__id: 1,
    visitors__created_at_day: new Date('2017-01-03T00:00:00.000Z')
  }, {
    visitors__id: 2,
    visitors__created_at_day: new Date('2017-01-05T00:00:00.000Z')
  }, {
    visitors__id: 3,
    visitors__created_at_day: new Date('2017-01-06T00:00:00.000Z')
  }, {
    visitors__id: 4,
    visitors__created_at_day: new Date('2017-01-07T00:00:00.000Z')
  }, {
    visitors__id: 5,
    visitors__created_at_day: new Date('2017-01-07T00:00:00.000Z')
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
    visitors__created_at_day: new Date('2017-01-07T00:00:00.000Z')
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
    visitors__created_at_day: new Date('2016-09-07T00:00:00.000Z')
  }, {
    visitors__created_at_day: new Date('2017-01-03T00:00:00.000Z')
  }, {
    visitors__created_at_day: new Date('2017-01-05T00:00:00.000Z')
  }, {
    visitors__created_at_day: new Date('2017-01-06T00:00:00.000Z')
  }, {
    visitors__created_at_day: new Date('2017-01-07T00:00:00.000Z')
  }, {
    visitors__created_at_day: new Date('2017-01-07T00:00:00.000Z')
  }]));

  it('ungrouped false without id', () => runQueryTest({
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
    ungrouped: false
  }, [{
    visitors__created_at_day: new Date('2016-09-07T00:00:00.000Z')
  }, {
    visitors__created_at_day: new Date('2017-01-03T00:00:00.000Z')
  }, {
    visitors__created_at_day: new Date('2017-01-05T00:00:00.000Z')
  }, {
    visitors__created_at_day: new Date('2017-01-06T00:00:00.000Z')
  }, {
    visitors__created_at_day: new Date('2017-01-07T00:00:00.000Z')
  }]));
});
