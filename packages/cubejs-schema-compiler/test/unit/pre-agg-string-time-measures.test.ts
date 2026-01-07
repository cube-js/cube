import { prepareJsCompiler } from './PrepareCompiler';
import { PostgresQuery } from '../../src';

describe('pre-aggregations with string, time, and boolean measures', () => {
  it('should use MAX aggregation for string measures in pre-aggregations', async () => {
    const { compiler, cubeEvaluator, joinGraph } = prepareJsCompiler(
      `
        cube(\`Events\`, {
          sql: \`SELECT * FROM events\`,

          preAggregations: {
            eventRollup: {
              dimensions: [CUBE.category],
              measures: [CUBE.eventIds]
            }
          },

          measures: {
            eventIds: {
              type: 'string',
              sql: \`array_agg(\${CUBE.id})\`
            },
            count: {
              type: 'count'
            }
          },

          dimensions: {
            id: {
              sql: \`id\`,
              type: 'string',
              primaryKey: true
            },
            category: {
              sql: \`category\`,
              type: 'string'
            }
          }
        });
      `
    );

    await compiler.compile();

    const query = new PostgresQuery(
      { joinGraph, cubeEvaluator, compiler },
      {
        measures: ['Events.eventIds'],
        dimensions: ['Events.category'],
        timeDimensions: [],
        filters: [],
        having: [],
        orders: [],
        limit: null,
        offset: null,
        responseFormat: 'compact'
      }
    );

    const preAggregations = query.preAggregations.preAggregationsDescription();
    expect(preAggregations.length).toBeGreaterThan(0);

    const aggregationsColumns = preAggregations[0]?.aggregationsColumns;
    expect(aggregationsColumns).toBeDefined();
  });

  it('should handle cumulative sums, string, booleans, and time measures in pre-aggregation', async () => {
    const { compiler, cubeEvaluator, joinGraph } = prepareJsCompiler(
      `
        cube(\`Orders\`, {
          sql: \`SELECT * FROM orders\`,

          preAggregations: {
            ordersRollup: {
              dimensions: [CUBE.status],
              measures: [
                CUBE.cumulativeNotes,
                CUBE.cumulativeBoolean,
                CUBE.cumulativeSum,
                CUBE.cumulativeTime
              ]
            }
          },

          measures: {
            cumulativeNotes: {
              type: 'string',
              sql: \`array_agg(DISTINCT \${CUBE.notes})\`
            },
            cumulativeBoolean: {
              type: 'boolean',
              sql: \`BOOL_OR(\${CUBE.status})\`
            },
            cumulativeSum: {
              type: 'sum',
              sql: \`1\`
            },
            cumulativeTime: {
              type: 'time',
              sql: \`MAX(CURRENT_DATE)\`
            },
            count: {
              type: 'count'
            }
          },

          dimensions: {
            id: {
              sql: \`id\`,
              type: 'number',
              primaryKey: true
            },
            status: {
              sql: \`status\`,
              type: 'string'
            },
            notes: {
              sql: \`notes\`,
              type: 'string'
            }
          }
        });
      `
    );

    await compiler.compile();

    const query = new PostgresQuery(
      { joinGraph, cubeEvaluator, compiler },
      {
        measures: [
          'Orders.cumulativeNotes',
          'Orders.cumulativeSum', 
          'Orders.cumulativeBoolean',
          'Orders.cumulativeTime'
        ],
        dimensions: ['Orders.status'],
        timeDimensions: [],
        filters: [],
        having: [],
        orders: [],
        limit: null,
        offset: null,
        responseFormat: 'compact'
      }
    );

    const [sql, params] = query.buildSqlAndParams();
    console.log('Cumulative string measure SQL:', sql);
    expect(sql.includes('max("orders__cumulative_notes")')).toBe(true);
    expect(sql.includes('sum("orders__cumulative_sum")')).toBe(true);
    expect(sql.includes('max("orders__cumulative_boolean")')).toBe(true);
    expect(sql.includes('max("orders__cumulative_time")')).toBe(true);
    expect([sql, params]).toBeDefined();
  });
});
