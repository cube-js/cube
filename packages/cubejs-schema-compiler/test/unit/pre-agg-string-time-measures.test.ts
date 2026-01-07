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
    
    const aggregationsColumns = preAggregations[0].aggregationsColumns;
    expect(aggregationsColumns).toBeDefined();
    expect(aggregationsColumns.some((col: string) => col.includes('max('))).toBe(true);
  });

  it('should use MAX aggregation for time measures in pre-aggregations', async () => {
    const { compiler, cubeEvaluator, joinGraph } = prepareJsCompiler(
      `
        cube(\`Transactions\`, {
          sql: \`SELECT * FROM transactions\`,

          preAggregations: {
            transactionRollup: {
              dimensions: [CUBE.status],
              measures: [CUBE.maxTimestamp]
            }
          },

          measures: {
            maxTimestamp: {
              type: 'time',
              sql: \`MAX(\${CUBE.timestamp})\`
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
            timestamp: {
              sql: \`timestamp\`,
              type: 'time'
            }
          }
        });
      `
    );

    await compiler.compile();

    const query = new PostgresQuery(
      { joinGraph, cubeEvaluator, compiler },
      {
        measures: ['Transactions.maxTimestamp'],
        dimensions: ['Transactions.status'],
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
    
    const aggregationsColumns = preAggregations[0].aggregationsColumns;
    expect(aggregationsColumns).toBeDefined();
    expect(aggregationsColumns.some((col: string) => col.includes('max('))).toBe(true);
  });

  it('should use MAX aggregation for boolean measures in pre-aggregations', async () => {
    const { compiler, cubeEvaluator, joinGraph } = prepareJsCompiler(
      `
        cube(\`Flags\`, {
          sql: \`SELECT * FROM flags\`,

          preAggregations: {
            flagRollup: {
              dimensions: [CUBE.category],
              measures: [CUBE.isActive]
            }
          },

          measures: {
            isActive: {
              type: 'boolean',
              sql: \`MAX(CASE WHEN \${CUBE.active} = true THEN 1 ELSE 0 END)\`
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
            category: {
              sql: \`category\`,
              type: 'string'
            },
            active: {
              sql: \`active\`,
              type: 'boolean'
            }
          }
        });
      `
    );

    await compiler.compile();

    const query = new PostgresQuery(
      { joinGraph, cubeEvaluator, compiler },
      {
        measures: ['Flags.isActive'],
        dimensions: ['Flags.category'],
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
    
    const aggregationsColumns = preAggregations[0].aggregationsColumns;
    expect(aggregationsColumns).toBeDefined();
    expect(aggregationsColumns.some((col: string) => col.includes('max('))).toBe(true);
  });

  it('should generate correct SQL for pre-aggregation with string measure', async () => {
    const { compiler, cubeEvaluator, joinGraph } = prepareJsCompiler(
      `
        cube(\`Products\`, {
          sql: \`SELECT * FROM products\`,

          preAggregations: {
            productRollup: {
              dimensions: [CUBE.category],
              measures: [CUBE.tagList]
            }
          },

          measures: {
            tagList: {
              type: 'string',
              sql: \`array_agg(DISTINCT \${CUBE.tag})\`
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
            },
            tag: {
              sql: \`tag\`,
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
        measures: ['Products.tagList'],
        dimensions: ['Products.category'],
        timeDimensions: [],
        filters: [],
        having: [],
        orders: [],
        limit: null,
        offset: null,
        responseFormat: 'compact'
      }
    );

    expect(() => query.buildSqlAndParams()).not.toThrow();
  });

  it('should generate correct SQL for pre-aggregation with boolean measure', async () => {
    const { compiler, cubeEvaluator, joinGraph } = prepareJsCompiler(
      `
        cube(\`Settings\`, {
          sql: \`SELECT * FROM settings\`,

          preAggregations: {
            settingsRollup: {
              dimensions: [CUBE.userId],
              measures: [CUBE.hasNotifications]
            }
          },

          measures: {
            hasNotifications: {
              type: 'boolean',
              sql: \`BOOL_OR(\${CUBE.notifications_enabled})\`
            }
          },

          dimensions: {
            id: {
              sql: \`id\`,
              type: 'number',
              primaryKey: true
            },
            userId: {
              sql: \`user_id\`,
              type: 'number'
            },
            notifications_enabled: {
              sql: \`notifications_enabled\`,
              type: 'boolean'
            }
          }
        });
      `
    );

    await compiler.compile();

    const query = new PostgresQuery(
      { joinGraph, cubeEvaluator, compiler },
      {
        measures: ['Settings.hasNotifications'],
        dimensions: ['Settings.userId'],
        timeDimensions: [],
        filters: [],
        having: [],
        orders: [],
        limit: null,
        offset: null,
        responseFormat: 'compact'
      }
    );

    expect(() => query.buildSqlAndParams()).not.toThrow();
  });
});
