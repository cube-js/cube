import { CompileError } from '../../../src/compiler/CompileError';
import { ClickHouseQuery } from '../../../src/adapter/ClickHouseQuery';
import { prepareCompiler } from '../../../src/compiler/PrepareCompiler';

import { prepareCompiler as testPrepareCompiler } from '../../unit/PrepareCompiler';
import { ClickHouseDbRunner } from './ClickHouseDbRunner';
import { logSqlAndParams } from '../../unit/TestUtil';

describe('ClickHouse DataSchemaCompiler', () => {
  jest.setTimeout(200000);

  const dbRunner = new ClickHouseDbRunner();

  afterAll(async () => {
    await dbRunner.tearDown();
  });

  it('gutter', () => {
    const { compiler } = testPrepareCompiler(`
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
    const { compiler } = testPrepareCompiler(`
    cube({}, {
      measures: {}
    })
    `);
    return compiler.compile()
      .then(() => {
        compiler.throwIfAnyErrors();
        throw new Error();
      })
      .catch((error: any) => {
        expect(error).toBeInstanceOf(CompileError);
      });
  });

  it('duplicate member', () => {
    const { compiler } = testPrepareCompiler(`
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
    }).catch((error: any) => {
      expect(error).toBeInstanceOf(CompileError);
    });
  });

  it('calculated metrics', () => {
    const { compiler, cubeEvaluator, joinGraph } = testPrepareCompiler(`
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
      const query = new ClickHouseQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: ['visitors.visitor_count'],
        timeDimensions: [{
          dimension: 'visitors.created_at',
          granularity: 'day',
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

      logSqlAndParams(query);

      return dbRunner.testQuery(query.buildSqlAndParams()).then((res: any) => {
        expect(res).toEqual(
          [
            { visitors__created_at_day: '2017-01-02T00:00:00.000', visitors__visitor_count: '1' },
            { visitors__created_at_day: '2017-01-04T00:00:00.000', visitors__visitor_count: '1' },
            { visitors__created_at_day: '2017-01-05T00:00:00.000', visitors__visitor_count: '1' },
            { visitors__created_at_day: '2017-01-06T00:00:00.000', visitors__visitor_count: '2' }
          ]
        );
      });
    });

    return result;
  });

  it('static dimension case', async () => {
    const { compiler, cubeEvaluator, joinGraph } = testPrepareCompiler(`
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
    await compiler.compile();

    const query = new ClickHouseQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures: ['visitors.visitor_count'],
      dimensions: ['visitors.status'],
      timezone: 'America/Los_Angeles',
      order: [{
        id: 'visitors.status'
      }]
    });

    logSqlAndParams(query);

    return dbRunner.testQuery(query.buildSqlAndParams()).then((res: any) => {
      expect(res).toEqual(
        [
          { visitors__status: 'Approved', visitors__visitor_count: '2' },
          { visitors__status: 'Canceled', visitors__visitor_count: '4' }
        ]
      );
    });
  });

  it('dynamic dimension case', () => {
    const { compiler, cubeEvaluator, joinGraph } = testPrepareCompiler(`
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
        source: {
          type: 'string',
          sql: 'source'
        },
        latitude: {
          type: 'string',
          sql: 'latitude'
        },
        enabled_source: {
          type: 'string',
          case: {
            when: [{
              sql: \`\${CUBE}.status = 3\`,
              label: 'three'
            }, {
              sql: \`\${CUBE}.status = 2\`,
              label: {
                sql: \`\${CUBE}.source\`
              }
            }],
            else: {
              label: {
                sql: \`\${CUBE}.source\`
              }
            }
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
      const query = new ClickHouseQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: ['visitors.visitor_count'],
        dimensions: ['visitors.enabled_source'],
        timezone: 'America/Los_Angeles',
        order: [{
          id: 'visitors.enabled_source'
        }]
      });
      logSqlAndParams(query);

      return dbRunner.testQuery(query.buildSqlAndParams()).then((res: any) => {
        expect(res).toEqual(
          [
            { visitors__enabled_source: 'google', visitors__visitor_count: '1' },
            { visitors__enabled_source: 'some', visitors__visitor_count: '2' },
            { visitors__enabled_source: null, visitors__visitor_count: '3' },
          ]
        );
      });
    });

    return result;
  });

  {
    const { compiler, cubeEvaluator, joinGraph } = testPrepareCompiler(`
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
      [{ visitors__created_at: '2017-01-02T16:00:00.000' }],
      [
        { visitors__created_at: '2016-09-06T16:00:00.000' },
        { visitors__created_at: '2017-01-04T16:00:00.000' },
        { visitors__created_at: '2017-01-05T16:00:00.000' },
        { visitors__created_at: '2017-01-06T16:00:00.000' }
      ],
      [{ visitors__created_at: '2017-01-06T16:00:00.000' }],
      [
        { visitors__created_at: '2016-09-06T16:00:00.000' },
        { visitors__created_at: '2017-01-02T16:00:00.000' },
        { visitors__created_at: '2017-01-04T16:00:00.000' },
        { visitors__created_at: '2017-01-05T16:00:00.000' }
      ],
      [{ visitors__created_at: '2017-01-06T16:00:00.000' }]
    ];
    ['in_date_range', 'not_in_date_range', 'on_the_date', 'before_date', 'after_date'].map((operator, index) => {
      const filterValues = index < 2 ? ['2017-01-01', '2017-01-03'] : ['2017-01-06', '2017-01-06'];
      it(`filtered dates ${operator}`, async () => {
        await compiler.compile();

        const query = new ClickHouseQuery({ joinGraph, cubeEvaluator, compiler }, {
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
        logSqlAndParams(query);
        const res = await dbRunner.testQuery(query.buildSqlAndParams());

        expect(res).toEqual(responses[index]);
      });
      return true;
    });
  }

  it('export import', () => {
    const { compiler, cubeEvaluator, joinGraph } = prepareCompiler({
      dataSchemaFiles: () => Promise.resolve([
        {
          fileName: 'main.js',
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
          fileName: 'some.js',
          content: `
          export const foo = 'bar';
          `
        }
      ])
    }, { adapter: dbRunner.adapter });
    return compiler.compile().then(() => {
      const query = new ClickHouseQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: ['Main.count'],
        dimensions: [],
        timeDimensions: [],
        order: [],
        timezone: 'America/Los_Angeles'
      });
      logSqlAndParams(query);
      expect(query.buildSqlAndParams()[0]).toMatch(/bar/);
    });
  });

  it('contexts', () => {
    const { compiler, contextEvaluator } = testPrepareCompiler(`
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
      expect(contextEvaluator.contextList).toEqual(
        ['Marketing']
      );
    });
  });

  it('dashboard templates', () => {
    const { compiler, dashboardTemplateEvaluator } = testPrepareCompiler(`
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
      expect(JSON.parse(JSON.stringify(dashboardTemplateEvaluator.compiledTemplates))).toEqual(
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
      );
    });
  });
});
