/* globals it, describe, after */
/* eslint-disable quote-props */
const UserError = require('../compiler/UserError');
const PostgresQuery = require('../adapter/PostgresQuery');
const BigqueryQuery = require('../adapter/BigqueryQuery');
const PrepareCompiler = require('./PrepareCompiler');
require('should');

const { prepareCompiler } = PrepareCompiler;
const dbRunner = require('./DbRunner');

describe('SQL Generation', function test() {
  this.timeout(90000);

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
    `);

    
  it('filter with operator OR', () => {
    const result = compiler.compile().then(() => {
      let query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: [
          'visitor_checkins.google_sourced_checkins'
        ],
        timeDimensions: [],
        filters: [
          {
            or : [ 
              { dimension: 'cards.id', operator: 'equals', values: ['3'] },
              { dimension: 'cards.id', operator: 'equals', values: ['1'] }
            ]
          }, 
        ],
        timezone: 'America/Los_Angeles'
      });

      console.log(query.buildSqlAndParams());

      return dbRunner.testQuery(query.buildSqlAndParams()).then(res => {
        console.log(JSON.stringify(res));
        res.should.be.deepEqual(
          [{ "visitor_checkins__google_sourced_checkins": "1" }]
        );
      });
    });

    return result;
  });
 

  it('having and where filter in same operator OR', () => {
    return compiler.compile().then(() => {
      try{
        const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
          "measures": [
            "visitors.visitor_count"
          ], 
          "order": {
            "visitors.visitor_count": "desc"
          },
          "filters": [
            {
              or:[
                {
                  "dimension": "visitors.visitor_count",
                  "operator": "gt",
                  "values": [
                    "1"
                  ]
                },
                {
                  "dimension": "visitors.source",
                  "operator": "equals",
                  "values": [
                    "google"
                  ]
                }
              ]
            },
          ],
          "dimensions": [
            "visitors.source"
          ]
        });
  
        throw new Error();
      }catch(error){
        // You cannot use dimension and measure in same condition
        error.should.be.instanceof(UserError);
      }
    });
  });
  
  it('having filter with operator OR', () => {
    return compiler.compile().then(() => {
      const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: [
          'visitors.visitor_count',
          "cards.count",
          "visitors.averageCheckins",
        ],
        dimensions: [
          'visitors.source'
        ],
        timeDimensions: [],
        timezone: 'America/Los_Angeles',
        filters: [{ 
          or: [
            {
              dimension: 'cards.count',
              operator: 'equals',
              values: ['2']
            },
            {
              dimension: 'visitors.averageCheckins',
              operator: 'equals',
              values: ['2']
            }
          ]
        }],
        order: [{
          id: 'visitors.source'
        }]
      });

      console.log(query.buildSqlAndParams());

      return dbRunner.testQuery(query.buildSqlAndParams()).then(res => {
        console.log(JSON.stringify(res));
        res.should.be.deepEqual(
          [{
            "cards__count": "1",
            "visitors__source": "google",
            "visitors__visitor_count": "1",
            "visitors__average_checkins": "2.0000000000000000"
          }, {
            "cards__count": "2",
            "visitors__source": "some",
            "visitors__visitor_count": "1",
            "visitors__average_checkins":"6.0000000000000000"
          }]
        );
      });
    });
  });


  it('having filter with operators OR & AND', () => {
    return compiler.compile().then(() => {
      const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: [
          'visitors.visitor_count',
          "cards.count",
          "visitors.averageCheckins",
        ],
        dimensions: [
          'visitors.source'
        ],
        timeDimensions: [],
        timezone: 'America/Los_Angeles',
        filters: [{
          or: [
            { 
              and: [
                {
                  dimension: 'cards.count',
                  operator: 'equals',
                  values: ['2']
                },
                {
                  dimension: 'visitors.averageCheckins',
                  operator: 'equals',
                  values: ['6']
                }
              ]
            },
            { 
              and: [
                {
                  dimension: 'cards.count',
                  operator: 'equals',
                  values: ['1']
                },
                {
                  dimension: 'visitors.averageCheckins',
                  operator: 'equals',
                  values: ['2']
                }
              ]
            }
          ]
        }],
        order: [{
          id: 'visitors.source'
        }]
      });

      console.log(query.buildSqlAndParams());

      return dbRunner.testQuery(query.buildSqlAndParams()).then(res => {
        console.log(JSON.stringify(res));
        res.should.be.deepEqual(
          [{
            "cards__count": "1",
            "visitors__source": "google",
            "visitors__visitor_count": "1",
            "visitors__average_checkins": "2.0000000000000000"
          }, {
            "cards__count": "2",
            "visitors__source": "some",
            "visitors__visitor_count": "1",
            "visitors__average_checkins":"6.0000000000000000"
          }]
        );
      });
    });
  });

  it('having filter with operators OR & AND (with filter based on measures not from select part clause)', () => {
    return compiler.compile().then(() => {
      const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: [
          'visitors.visitor_count',
          // "cards.count",
          "visitors.averageCheckins",
        ],
        dimensions: [
          'visitors.source'
        ],
        timeDimensions: [],
        timezone: 'America/Los_Angeles',
        filters: [{
          or: [
            { 
              and: [
                {
                  dimension: 'visitors.averageCheckins',
                  operator: 'equals',
                  values: ['6']
                },
                {
                  dimension: 'cards.count',
                  operator: 'equals',
                  values: ['2']
                },
              ]
            },
            { 
              and: [
                {
                  dimension: 'visitors.averageCheckins',
                  operator: 'equals',
                  values: ['2']
                },
                {
                  dimension: 'cards.count',
                  operator: 'equals',
                  values: ['1']
                },
              ]
            }
          ]
        }],
        order: [{
          id: 'visitors.source'
        }]
      });

      console.log(query.buildSqlAndParams());

      return dbRunner.testQuery(query.buildSqlAndParams()).then(res => {
        console.log(JSON.stringify(res));
        res.should.be.deepEqual(
          [{
            // "cards__count": "1",
            "visitors__source": "google",
            "visitors__visitor_count": "1",
            "visitors__average_checkins": "2.0000000000000000"
          }, {
            // "cards__count": "2",
            "visitors__source": "some",
            "visitors__visitor_count": "1",
            "visitors__average_checkins":"6.0000000000000000"
          }]
        );
      });
    });
  });

  it('where filter with operators OR & AND', () => {
    return compiler.compile().then(() => {
      const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: [
          "visitors.visitor_count"
        ],
        dimensions: [
          "visitors.source",
          "visitor_checkins.cardsCount"
        ],
        timeDimensions: [],
        timezone: 'America/Los_Angeles',
        filters: [{
          or: [
            {
              and: [
                {
                  dimension: 'visitors.source',
                  operator: 'equals',
                  values: ['some']
                },
                {
                  dimension: 'visitor_checkins.cardsCount',
                  operator: 'equals',
                  values: ['0']
                },
              ]
            },
            {
              and: [
                {
                  dimension: 'visitors.source',
                  operator: 'equals',
                  values: ['google']
                },
                {
                  dimension: 'visitor_checkins.cardsCount',
                  operator: 'equals',
                  values: ['1']
                },
              ]
            }
          ]
        }],
        order: [{
          "visitors.visitor_count": "desc"
        }]
      });

      console.log(query.buildSqlAndParams());

      return dbRunner.testQuery(query.buildSqlAndParams()).then(res => {
        console.log(JSON.stringify(res));
        res.should.be.deepEqual(
          [{ 
            "visitors__source": "google", 
            "visitor_checkins__cards_count": "1",
            "visitors__visitor_count": "1",
          }, { 
            "visitors__source": "some",
            "visitors__visitor_count": "2",
            "visitor_checkins__cards_count": "0"
          }]
        );
      });
    });
  });

  it('where filter with operators OR & AND (with filter based on dimensions not from select part clause)', () => {
    return compiler.compile().then(() => {
      const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: [
          "visitors.visitor_count"
        ],
        dimensions: [
          "visitors.source", 
        ],
        timeDimensions: [],
        timezone: 'America/Los_Angeles',
        filters: [{
          or: [
            {
              and: [
                {
                  dimension: 'visitors.source',
                  operator: 'equals',
                  values: ['some']
                },
                {
                  dimension: 'visitor_checkins.cardsCount',
                  operator: 'equals',
                  values: ['0']
                },
              ]
            },
            {
              and: [
                {
                  dimension: 'visitors.source',
                  operator: 'equals',
                  values: ['google']
                },
                {
                  dimension: 'visitor_checkins.cardsCount',
                  operator: 'equals',
                  values: ['1']
                },
              ]
            }
          ]
        }],
        order: [{
          "visitors.visitor_count": "desc"
        }]
      });

      console.log(query.buildSqlAndParams());

      return dbRunner.testQuery(query.buildSqlAndParams()).then(res => {
        console.log(JSON.stringify(res));
        res.should.be.deepEqual(
          [{ 
            "visitors__source": "google",  
            "visitors__visitor_count": "1",
          }, { 
            "visitors__source": "some",
            "visitors__visitor_count": "2", 
          }]
        );
      });
    });
  });

  // end of tests
});