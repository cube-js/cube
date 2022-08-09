import { QueryAlias } from '@cubejs-backend/shared';
import { MssqlQuery } from '../../src/adapter/MssqlQuery';
import { prepareCompiler } from './PrepareCompiler';
import { createJoinedCubesSchema } from './utils';

describe('MssqlQuery', () => {
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

  const joinedSchemaCompilers = prepareCompiler(createJoinedCubesSchema());

  it('group by the date_from field on unbounded trailing windows',
    () => compiler.compile().then(() => {
      const query = new MssqlQuery(
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

      const queryAndParams = query.buildSqlAndParams();

      const queryString = queryAndParams[0];
      const lastGroupByIdx = queryString.lastIndexOf('GROUP BY');
      const queryCloseIdx = queryString.indexOf(')', lastGroupByIdx + 1);
      const finalGroupBy = queryString.substring(lastGroupByIdx, queryCloseIdx);

      expect(finalGroupBy).toEqual('GROUP BY "visitors.createdAt_series"."date_from"');
    }));

  it('should not include order by clauses in subqueries',
    () => compiler.compile().then(() => {
      const query = new MssqlQuery(
        { joinGraph, cubeEvaluator, compiler },
        {
          dimensions: ['SalesManagers.id', 'SalesManagers.dealsAmount'],
        }
      );

      const subQueryDimensions = query.collectFromMembers(
        false,
        query.collectSubQueryDimensionsFor.bind(query),
        'collectSubQueryDimensionsFor'
      );

      const queryAndParams = query.buildSqlAndParams();
      const subQuery: any = query.subQueryJoin(subQueryDimensions[0]);

      expect(/ORDER BY/.test(subQuery.sql)).toEqual(false);
      expect(queryAndParams[0]).toMatch(/ORDER BY/);
    }));

  it('aggregating on top of sub-queries', async () => {
    await joinedSchemaCompilers.compiler.compile();
    const query = new MssqlQuery({
      joinGraph: joinedSchemaCompilers.joinGraph,
      cubeEvaluator: joinedSchemaCompilers.cubeEvaluator,
      compiler: joinedSchemaCompilers.compiler,
    },
    {
      dimensions: ['E.eval'],
      measures: ['B.bval_sum'],
      order: [{ id: 'B.bval_sum' }],
    });
    const sql = query.buildSqlAndParams();
    // eslint-disable-next-line no-useless-escape
    const re = new RegExp(`(GROUP BY)(\n|.)+("${QueryAlias.AGG_SUB_QUERY_KEYS}"\."e__eval")`);
    expect(re.test(sql[0])).toBeTruthy();
  });
});
