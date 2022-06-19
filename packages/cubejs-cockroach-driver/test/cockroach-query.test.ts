// eslint-disable-next-line import/no-extraneous-dependencies
import { prepareCompiler as originalPrepareCompiler } from '@cubejs-backend/schema-compiler';

import { CockroachQuery } from '../src/CockroachQuery';

export const testCompiler = (content: any, options: any) => originalPrepareCompiler({
  localPath: () => __dirname,
  dataSchemaFiles: () => Promise.resolve([
    { fileName: 'main.js', content }
  ])
}, { adapter: 'cockroach', ...options });

describe('CockroachQuery', () => {
  const { compiler, joinGraph, cubeEvaluator } = testCompiler(`
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
          sql: 'created_at'
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
    `, {});

  it('test equal filters', async () => {
    await compiler.compile();

    const filterValuesVariants = [
      [[true], 'WHERE ("visitors".name = $1)'],
      [[false], 'WHERE ("visitors".name = $1)'],
      [[''], 'WHERE ("visitors".name = $1)'],
      [[null], 'WHERE ("visitors".name IS NULL)'],
    ];

    for (const [values, expected] of filterValuesVariants) {
      const query = new CockroachQuery({ joinGraph, cubeEvaluator, compiler }, {
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
});
