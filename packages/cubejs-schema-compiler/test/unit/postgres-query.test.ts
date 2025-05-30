/* eslint-disable no-restricted-syntax */
import { PostgresQuery } from '../../src/adapter/PostgresQuery';
import { prepareJsCompiler } from './PrepareCompiler';

describe('PostgresQuery', () => {
  const { compiler, joinGraph, cubeEvaluator } = prepareJsCompiler(`
    cube(\`visitors\`, {
      sql: \`
      select * from visitors
      \`,

      measures: {
        count: {
          type: 'count'
        },

        unboundedCount: {
          type: 'count',
          rollingWindow: {
            trailing: 'unbounded'
          }
        }
      },

      dimensions: {
        createdAt: {
          type: 'time',
          sql: 'created_at',
          granularities: {
            fiscal_year: {
              interval: '1 year',
              offset: '1 month',
            },
            fiscal_quarter: {
              interval: '1 quarter',
              offset: '1 month',
            },
            fiscal_quarter_no_offset: {
              interval: '1 quarter',
            },
          }
        },
        fiscalCreatedAtLabel: {
          type: 'string',
          sql: \`'FY' || (EXTRACT(YEAR FROM \${createdAt.fiscal_year}) + 1)  || ' Q' || (EXTRACT(QUARTER FROM \${createdAt.fiscal_quarter}))\`
        },
        name: {
          type: 'string',
          sql: 'name'
        }
      }
    })

    cube(\`Deals\`, {
      sql: \`select * from deals\`,

      measures: {
        amount: {
          sql: \`amount\`,
          type: \`sum\`
        }
      },

      dimensions: {
        salesManagerId: {
          sql: \`sales_manager_id\`,
          type: 'string',
          primaryKey: true
        }
      }
    })

    cube(\`SalesManagers\`, {
      sql: \`select * from sales_managers\`,

      joins: {
        Deals: {
          relationship: \`hasMany\`,
          sql: \`\${SalesManagers}.id = \${Deals}.sales_manager_id\`
        }
      },

      measures: {
        averageDealAmount: {
          sql: \`\${dealsAmount}\`,
          type: \`avg\`
        }
      },

      dimensions: {
        id: {
          sql: \`id\`,
          type: \`string\`,
          primaryKey: true
        },

        dealsAmount: {
          sql: \`\${Deals.amount}\`,
          type: \`number\`,
          subQuery: true
        }
      }
    });
    `);

  it('test equal filters', async () => {
    await compiler.compile();

    const filterValuesVariants = [
      [[true], 'WHERE ("visitors".name = $1)'],
      [[false], 'WHERE ("visitors".name = $1)'],
      [[''], 'WHERE ("visitors".name = $1)'],
      [[null], 'WHERE ("visitors".name IS NULL)'],
    ];

    for (const [values, expected] of filterValuesVariants) {
      const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: [
          'visitors.count'
        ],
        timeDimensions: [],
        filters: [{
          member: 'visitors.name',
          operator: 'equals',
          values
        }],
        timezone: 'America/Los_Angeles'
      });

      const queryAndParams = query.buildSqlAndParams();
      expect(queryAndParams[0]).toContain(expected);
    }
  });

  it('test compound time dimension with custom granularity', async () => {
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      dimensions: [
        'visitors.fiscalCreatedAtLabel'
      ],
      timezone: 'America/Los_Angeles'
    });

    const queryAndParams = query.buildSqlAndParams();
    expect(queryAndParams[0].split('AT TIME ZONE \'America/Los_Angeles\'').length).toEqual(3);
  });

  describe('order by clause', () => {
    it('multi granularity ordered by min granularity (auto)', async () => {
      await compiler.compile();

      let query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: [
          'visitors.count'
        ],
        timeDimensions: [{
          dimension: 'visitors.createdAt',
          granularity: 'month',
          dateRange: ['2020-01-01', '2020-12-31'],
        }, {
          dimension: 'visitors.createdAt',
          granularity: 'week',
          dateRange: ['2020-01-01', '2020-12-31'],
        }, {
          dimension: 'visitors.createdAt',
          granularity: 'fiscal_quarter',
          dateRange: ['2020-01-01', '2020-12-31'],
        }, {
          dimension: 'visitors.createdAt',
          dateRange: ['2020-01-01', '2020-12-31'],
        }],
        order: [{ id: 'visitors.createdAt', desc: false }],
        timezone: 'America/Los_Angeles'
      });

      let queryAndParams = query.buildSqlAndParams();
      expect(queryAndParams[0]).toContain('ORDER BY 2 ASC');

      query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: [
          'visitors.count'
        ],
        timeDimensions: [{
          dimension: 'visitors.createdAt',
          granularity: 'week',
          dateRange: ['2020-01-01', '2020-12-31'],
        }, {
          dimension: 'visitors.createdAt',
          granularity: 'fiscal_quarter',
          dateRange: ['2020-01-01', '2020-12-31'],
        }, {
          dimension: 'visitors.createdAt',
          granularity: 'month',
          dateRange: ['2020-01-01', '2020-12-31'],
        }, {
          dimension: 'visitors.createdAt',
          dateRange: ['2020-01-01', '2020-12-31'],
        }],
        order: [{ id: 'visitors.createdAt', desc: false }],
        timezone: 'America/Los_Angeles'
      });

      queryAndParams = query.buildSqlAndParams();
      expect(queryAndParams[0]).toContain('ORDER BY 1 ASC');

      query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: [
          'visitors.count'
        ],
        timeDimensions: [{
          dimension: 'visitors.createdAt',
          granularity: 'year',
          dateRange: ['2020-01-01', '2020-12-31'],
        }, {
          dimension: 'visitors.createdAt',
          granularity: 'fiscal_quarter_no_offset',
          dateRange: ['2020-01-01', '2020-12-31'],
        }, {
          dimension: 'visitors.createdAt',
          dateRange: ['2020-01-01', '2020-12-31'],
        }],
        order: [{ id: 'visitors.createdAt', desc: false }],
        timezone: 'America/Los_Angeles'
      });

      queryAndParams = query.buildSqlAndParams();
      expect(queryAndParams[0]).toContain('ORDER BY 2 ASC');
    });

    it('multi granularity ordered by specified granularity', async () => {
      await compiler.compile();

      let query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: [
          'visitors.count'
        ],
        timeDimensions: [{
          dimension: 'visitors.createdAt',
          granularity: 'month',
          dateRange: ['2020-01-01', '2020-12-31'],
        }, {
          dimension: 'visitors.createdAt',
          granularity: 'week',
          dateRange: ['2020-01-01', '2020-12-31'],
        }, {
          dimension: 'visitors.createdAt',
          dateRange: ['2020-01-01', '2020-12-31'],
        }],
        order: [{ id: 'visitors.createdAt.week', desc: false }],
        timezone: 'America/Los_Angeles'
      });

      let queryAndParams = query.buildSqlAndParams();
      expect(queryAndParams[0]).toContain('ORDER BY 2 ASC');

      query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: [
          'visitors.count'
        ],
        timeDimensions: [{
          dimension: 'visitors.createdAt',
          granularity: 'month',
          dateRange: ['2020-01-01', '2020-12-31'],
        }, {
          dimension: 'visitors.createdAt',
          granularity: 'week',
          dateRange: ['2020-01-01', '2020-12-31'],
        }, {
          dimension: 'visitors.createdAt',
          dateRange: ['2020-01-01', '2020-12-31'],
        }],
        order: [{ id: 'visitors.createdAt.month', desc: false }],
        timezone: 'America/Los_Angeles'
      });

      queryAndParams = query.buildSqlAndParams();
      expect(queryAndParams[0]).toContain('ORDER BY 1 ASC');

      query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: [
          'visitors.count'
        ],
        timeDimensions: [{
          dimension: 'visitors.createdAt',
          granularity: 'week',
          dateRange: ['2020-01-01', '2020-12-31'],
        }, {
          dimension: 'visitors.createdAt',
          granularity: 'month',
          dateRange: ['2020-01-01', '2020-12-31'],
        }, {
          dimension: 'visitors.createdAt',
          dateRange: ['2020-01-01', '2020-12-31'],
        }],
        order: [{ id: 'visitors.createdAt.month', desc: false }],
        timezone: 'America/Los_Angeles'
      });

      queryAndParams = query.buildSqlAndParams();
      expect(queryAndParams[0]).toContain('ORDER BY 2 ASC');

      query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: [
          'visitors.count'
        ],
        timeDimensions: [{
          dimension: 'visitors.createdAt',
          granularity: 'week',
          dateRange: ['2020-01-01', '2020-12-31'],
        }, {
          dimension: 'visitors.createdAt',
          granularity: 'month',
          dateRange: ['2020-01-01', '2020-12-31'],
        }, {
          dimension: 'visitors.createdAt',
          granularity: 'fiscal_quarter_no_offset',
          dateRange: ['2020-01-01', '2020-12-31'],
        }, {
          dimension: 'visitors.createdAt',
          dateRange: ['2020-01-01', '2020-12-31'],
        }],
        order: [{ id: 'visitors.createdAt.fiscal_quarter_no_offset', desc: false }],
        timezone: 'America/Los_Angeles'
      });

      queryAndParams = query.buildSqlAndParams();
      expect(queryAndParams[0]).toContain('ORDER BY 3 ASC');
    });
  });
});
