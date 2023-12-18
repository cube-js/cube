import { QueryAlias } from '@cubejs-backend/shared';
import { OracleQuery } from '../../src/adapter/OracleQuery';
import { prepareCompiler } from './PrepareCompiler';
import { createJoinedCubesSchema } from './utils';

describe('OracleQuery', () => {
  const { compiler, joinGraph, cubeEvaluator } = prepareCompiler(`
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

        source: {
          type: 'string',
          sql: 'source'
        },
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

  const joinedSchemaCompilers = prepareCompiler(createJoinedCubesSchema());

  it('should cast date with correct oracle format',
    () => compiler.compile().then(() => {
      const query = new OracleQuery(
        { joinGraph, cubeEvaluator, compiler },
        {
          measures: ['visitors.count', 'visitors.unboundedCount'],
          timeDimensions: [
            {
              dimension: 'visitors.createdAt',
              granularity: 'week',
              dateRange: ['2017-01-01', '2017-01-30'],
            },
          ],
          timezone: 'America/Los_Angeles',
          order: [
            {
              id: 'visitors.createdAt',
            },
          ],
        }
      );

      expect(query.dateTimeCast('12-25-2005')).toEqual(`to_date(:"12-25-2005", 'YYYY-MM-DD"T"HH24:MI:SS****"Z"')`);
    }));

    it('should correctly truncate date to a quarter',
        () => compiler.compile().then(() => {
            const query = new OracleQuery(
                { joinGraph, cubeEvaluator, compiler },
                {
                    measures: ['visitors.count', 'visitors.unboundedCount'],
                    timeDimensions: [
                        {
                            dimension: 'visitors.createdAt',
                            granularity: 'week',
                            dateRange: ['2017-01-01', '2017-01-30'],
                        },
                    ],
                    timezone: 'America/Los_Angeles',
                    order: [
                        {
                            id: 'visitors.createdAt',
                        },
                    ],
                }
            );

            expect(query.timeGroupedColumn('test', 'quarter')).toEqual(`TRUNC(test, 'Q')`);
        }));

});
