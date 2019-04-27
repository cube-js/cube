const CompileError = require('../compiler/CompileError');
const PostgresQuery = require('../adapter/PostgresQuery');
const PrepareCompiler = require('./PrepareCompiler');
const MainPrepareCompiler = require('../compiler/PrepareCompiler');
require('should');

const prepareCompiler = PrepareCompiler.prepareCompiler;
const dbRunner = require('./DbRunner');

describe('DataSchemaCompiler', () => {
  it('gutter', () => {
    const { compiler } = prepareCompiler(`
    cube('visitors', {
      sql: \`
      select * from visitors
      \`,

      measures: {
        visitor_count: {
          type: 'count',
          sql: 'id',
          drillMembers: [source, created_at]
        },
        visitor_revenue: {
          type: 'sum',
          sql: 'amount',
          drillMemberReferences: [source, created_at]
        }
      },

      dimensions: {
        source: {
          type: 'string',
          sql: 'source'
        },
        created_at: {
          type: 'time',
          sql: 'created_at'
        }
      }
    })
    `);
    return compiler.compile();
  });

  it('error', () => {
    const { compiler } = prepareCompiler(`
    cube({}, {
      measures: {}
    })
    `);
    return compiler.compile()
      .then(() => {
        compiler.throwIfAnyErrors();
        throw new Error();
      })
      .catch((error) => {
        error.should.be.instanceof(CompileError);
      });
  });

  it('duplicate member', () => {
    const { compiler } = prepareCompiler(`
    cube('visitors', {
      sql: \`
      select * from visitors
      \`,

      measures: {
        count: {
          type: 'count',
          sql: 'id'
        },
        id: {
          type: 'sum',
          sql: 'id'
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
    return compiler.compile().then(() => {
      compiler.throwIfAnyErrors();
      throw new Error();
    }).catch((error) => {
      error.should.be.instanceof(CompileError);
    });
  });

  it('calculated metrics', () => {
    const { compiler, transformer, cubeEvaluator, joinGraph } = prepareCompiler(`
    cube('visitors', {
      sql: \`
      select * from visitors
      \`,

      measures: {
        visitor_count: {
          type: 'count',
          sql: 'id'
        },
        visitor_revenue: {
          type: 'sum',
          sql: 'amount'
        },
        per_visitor_revenue: {
          type: 'number',
          sql: visitor_revenue + "/" + visitor_count
        }
      },

      dimensions: {
        source: {
          type: 'string',
          sql: 'source'
        },
        created_at: {
          type: 'time',
          sql: 'created_at'
        },
        updated_at: {
          type: 'time',
          sql: 'updated_at'
        }
      }
    })
    `);
    const result = compiler.compile().then(() => {
      const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: ['visitors.visitor_count'],
        timeDimensions: [{
          dimension: 'visitors.created_at',
          granularity: 'date',
          dateRange: ['2017-01-01', '2017-01-30']
        }],
        filters: [{
          dimension: 'visitors.updated_at',
          operator: 'in_date_range',
          values: ['2017-01-01', '2017-01-30']
        }],
        order: [{
          id: 'visitors.created_at'
        }],
        timezone: 'America/Los_Angeles'
      });

      console.log(query.buildSqlAndParams());
      return dbRunner.testQuery(query.buildSqlAndParams()).then(res => {
        res.should.be.deepEqual(
          [
            { "visitors.created_at_date": "2017-01-02T00:00:00.000Z", "visitors.visitor_count": "1" },
            { "visitors.created_at_date": "2017-01-04T00:00:00.000Z", "visitors.visitor_count": "1" },
            { "visitors.created_at_date": "2017-01-05T00:00:00.000Z", "visitors.visitor_count": "1" },
            { "visitors.created_at_date": "2017-01-06T00:00:00.000Z", "visitors.visitor_count": "2" }
          ]
        );
      });
    });

    return result;
  });

  it('dimension case', () => {
    const { compiler, transformer, cubeEvaluator, joinGraph } = prepareCompiler(`
    cube('visitors', {
      sql: \`
      select * from visitors
      \`,

      measures: {
        visitor_count: {
          type: 'count',
          sql: 'id'
        }
      },

      dimensions: {
        status: {
          type: 'string',
          case: {
            when: [{
              sql: \`\${CUBE}.status = 1\`,
              label: 'Approved'
            }, {
              sql: \`\${CUBE}.status = 2\`,
              label: 'Canceled'
            }],
            else: { label: 'Unknown' }
          }
        },
        created_at: {
          type: 'time',
          sql: 'created_at'
        }
      }
    })
    `);
    const result = compiler.compile().then(() => {
      const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: ['visitors.visitor_count'],
        dimensions: ['visitors.status'],
        timezone: 'America/Los_Angeles',
        order: [{
          id: 'visitors.status'
        }]
      });

      console.log(query.buildSqlAndParams());

      return dbRunner.testQuery(query.buildSqlAndParams()).then(res => {
        res.should.be.deepEqual(
          [
            { "visitors.status": "Approved", "visitors.visitor_count": "2" },
            { "visitors.status": "Canceled", "visitors.visitor_count": "4" }
          ]
        );
      });
    });

    return result;
  });
  it('filtered dates', () => {
    const { compiler, cubeEvaluator, joinGraph } = prepareCompiler(`
    cube('visitors', {
      sql: \`
      select * from visitors
      \`,

      dimensions: {
        source: {
          type: 'string',
          sql: 'source'
        },
        created_at: {
          type: 'time',
          sql: 'created_at'
        },
        updated_at: {
          type: 'time',
          sql: 'updated_at'
        }
      }
    })
    `);
    const responses = [
      [{ 'visitors.created_at': '2017-01-03T00:00:00.000Z' }],
      [
        { 'visitors.created_at': '2016-09-07T00:00:00.000Z' },
        { 'visitors.created_at': '2017-01-05T00:00:00.000Z' },
        { 'visitors.created_at': '2017-01-06T00:00:00.000Z' },
        { 'visitors.created_at': '2017-01-07T00:00:00.000Z' }
      ],
      [{ 'visitors.created_at': '2017-01-07T00:00:00.000Z' }],
      [
        { 'visitors.created_at': '2016-09-07T00:00:00.000Z' },
        { 'visitors.created_at': '2017-01-03T00:00:00.000Z' },
        { 'visitors.created_at': '2017-01-05T00:00:00.000Z' },
        { 'visitors.created_at': '2017-01-06T00:00:00.000Z' }
      ],
      [{ 'visitors.created_at': '2017-01-07T00:00:00.000Z' }]
    ];
    const result = compiler.compile().then(() => {
      const queries = ['in_date_range', 'not_in_date_range', 'on_the_date', 'before_date', 'after_date'].map((operator, index) => {
        const filterValues = index < 2 ? ['2017-01-01', '2017-01-03'] : ['2017-01-06', '2017-01-06'];
        return new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
          measures: [],
          dimensions: ['visitors.created_at'],
          timeDimensions: [],
          filters: [{
            operator,
            dimension: 'visitors.created_at',
            values: filterValues
          }],
          order: [{
            id: 'visitors.created_at',
            desc: false
          }],
          timezone: 'America/Los_Angeles'
        });
      });

      return Promise.all(queries.map(async (query, index) => {
        console.log(query.buildSqlAndParams());
        const res = await dbRunner.testQuery(query.buildSqlAndParams());

        res.should.be.deepEqual(responses[index]);
      }));
    });

    return result;
  });

  it('export import', () => {
    const { compiler, cubeEvaluator, joinGraph } = MainPrepareCompiler.prepareCompiler({
      dataSchemaFiles: () => Promise.resolve([
        {
          fileName: "main.js",
          content: `
          const fooTable = require('./some.js').foo;
          cube('Main', {
            sql: \`select * from \${fooTable}\`,
            measures: {
              count: {
                sql: 'count(*)',
                type: 'number'
              }
            }
          })
          `
        }, {
          fileName: "some.js",
          content: `
          export const foo = 'bar';
          `
        }
      ])
    }, { adapter: 'postgres' });
    return compiler.compile().then(() => {
      const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: ['Main.count'],
        dimensions: [],
        timeDimensions: [],
        order: [],
        timezone: 'America/Los_Angeles'
      });
      console.log(query.buildSqlAndParams());
      query.buildSqlAndParams()[0].should.match(/bar/);
    });
  });

  it('contexts', () => {
    const { compiler, contextEvaluator } = prepareCompiler(`
      cube('Visitors', {
        sql: \`
        select * from visitors
        \`,

        measures: {
          visitor_count: {
            type: 'count',
            sql: 'id'
          },
        },

        dimensions: {
          source: {
            type: 'string',
            sql: 'source'
          },
        }
      })

      context('Marketing', {
        contextMembers: [Visitors]
      });
    `);
    return compiler.compile().then(() => {
      contextEvaluator.contextList.should.be.deepEqual(
        ['Marketing']
      )
    });
  });

  it('dashboard templates', () => {
    const { compiler, contextEvaluator, dashboardTemplateEvaluator } = prepareCompiler(`
      cube('Visitors', {
        sql: \`
        select * from visitors
        \`,

        measures: {
          count: {
            type: 'count',
            sql: 'id'
          },
        },

        dimensions: {
          source: {
            type: 'string',
            sql: 'source'
          },
          
          createdAt: {
            sql: 'created_at',
            type: 'time'
          }
        }
      })

      dashboardTemplate('VisitorsMarketing', {
        title: 'Marketing',
        items: [{
          measures: [Visitors.count],
          dimensions: [Visitors.source],
          visualization: { 
            type: 'pie' 
          },
          timeDimension: {
            dimension: Visitors.createdAt,
            dateRange: 'last week'
          },
          filters: [{
            member: Visitors.source,
            operator: 'equals',
            params: ['google']
          }],
          order: [{
            member: Visitors.source,
            direction: 'asc'
          }],
          layout: {
            w: 24,
            h: 4,
            x: 0,
            y: 0
          }
        }]
      });
    `);
    return compiler.compile().then(() => {
      JSON.parse(JSON.stringify(dashboardTemplateEvaluator.compiledTemplates)).should.be.deepEqual(
        [{
          name: 'VisitorsMarketing',
          title: 'Marketing',
          fileName: 'main.js',
          items: [{
            config: {
              visualization_type: 'pie',
              metrics: ['Visitors.count'],
              dimension: ['Visitors.source'],
              daterange: 'last week',
              time_dimension_field: 'Visitors.createdAt',
              order: [{ desc: false, id: 'Visitors.source' }],
              filters: [
                {
                  value: ['google'],
                  operator: 'equals',
                  dimension: 'Visitors.source'
                }
              ]
            },
            layout: { w: 24, h: 4, x: 0, y: 0 }
          }]
        }]
      )
    });
  });
});
