/* eslint-disable no-restricted-syntax */
import { PostgresQuery } from '../../src/adapter/PostgresQuery';
import { prepareJsCompiler } from './PrepareCompiler';

describe('Local Time Dimensions', () => {
  const { compiler, joinGraph, cubeEvaluator } = prepareJsCompiler(`
    cube(\`orders\`, {
      sql: \`
        SELECT * FROM orders
      \`,

      measures: {
        count: {
          type: 'count'
        }
      },

      dimensions: {
        created_at: {
          type: 'time',
          sql: 'created_at'
        },

        local_date: {
          type: 'time',
          sql: \`DATE(created_at AT TIME ZONE 'America/Los_Angeles')\`,
          localTime: true,
          description: 'Date in Pacific timezone'
        },

        local_hour: {
          type: 'time',
          sql: \`DATE_TRUNC('hour', created_at AT TIME ZONE 'America/Los_Angeles')\`,
          localTime: true,
          description: 'Hour in Pacific timezone'
        }
      }
    })
  `);

  describe('SQL generation', () => {
    it('should not apply convertTz to local time dimensions', async () => {
      await compiler.compile();

      const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: ['orders.count'],
        timeDimensions: [{
          dimension: 'orders.local_date',
          granularity: 'day',
          dateRange: ['2025-01-01', '2025-01-31']
        }],
        timezone: 'America/New_York'
      });

      const queryAndParams = query.buildSqlAndParams();
      const sql = queryAndParams[0];

      // Should not contain timezone conversion for local_date
      // The SQL should use the raw dimension SQL without AT TIME ZONE wrapping
      expect(sql).toContain('DATE(created_at AT TIME ZONE \'America/Los_Angeles\')');
      
      // Should not double-convert (shouldn't have AT TIME ZONE 'America/New_York' on local_date)
      const localDateMatches = sql.match(/DATE\(created_at AT TIME ZONE 'America\/Los_Angeles'\)/g);
      expect(localDateMatches).toBeTruthy();
    });

    it('should apply convertTz to regular time dimensions', async () => {
      await compiler.compile();

      const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: ['orders.count'],
        timeDimensions: [{
          dimension: 'orders.created_at',
          granularity: 'day',
          dateRange: ['2025-01-01', '2025-01-31']
        }],
        timezone: 'America/New_York'
      });

      const queryAndParams = query.buildSqlAndParams();
      const sql = queryAndParams[0];

      // Regular time dimension should have timezone conversion
      expect(sql).toContain('AT TIME ZONE \'America/New_York\'');
    });

    it('should support dateRange on local time dimensions', async () => {
      await compiler.compile();

      const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: ['orders.count'],
        timeDimensions: [{
          dimension: 'orders.local_date',
          dateRange: ['2025-01-01', '2025-01-31']
        }],
        timezone: 'UTC'
      });

      const queryAndParams = query.buildSqlAndParams();
      const sql = queryAndParams[0];

      // Should have WHERE clause with date range filter
      expect(sql).toMatch(/WHERE/i);
      // For localTime dimensions, parameters should NOT have timezone suffix
      expect(queryAndParams[1]).toContain('2025-01-01T00:00:00.000');
      expect(queryAndParams[1]).toContain('2025-01-31T23:59:59.999');
    });

    it('should support granularity on local time dimensions', async () => {
      await compiler.compile();

      const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: ['orders.count'],
        timeDimensions: [{
          dimension: 'orders.local_hour',
          granularity: 'hour',
          dateRange: ['2025-01-01', '2025-01-02']
        }],
        timezone: 'UTC'
      });

      const queryAndParams = query.buildSqlAndParams();
      const sql = queryAndParams[0];

      // Should apply granularity grouping
      expect(sql).toMatch(/DATE_TRUNC\('hour'/i);
    });

    it('should work with both local and regular time dimensions in same query', async () => {
      await compiler.compile();

      const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: ['orders.count'],
        dimensions: ['orders.local_date'],
        timeDimensions: [{
          dimension: 'orders.created_at',
          granularity: 'day',
          dateRange: ['2025-01-01', '2025-01-31']
        }],
        timezone: 'America/New_York'
      });

      const queryAndParams = query.buildSqlAndParams();
      const sql = queryAndParams[0];

      // local_date should not have New York timezone conversion
      expect(sql).toContain('DATE(created_at AT TIME ZONE \'America/Los_Angeles\')');
      
      // created_at should have New York timezone conversion
      expect(sql).toContain('AT TIME ZONE \'America/New_York\'');
    });
  });

  describe('Query features', () => {
    it('should support "last month" dateRange on local time dimensions', async () => {
      await compiler.compile();

      const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: ['orders.count'],
        timeDimensions: [{
          dimension: 'orders.local_date',
          dateRange: 'last month',
          granularity: 'day'
        }],
        timezone: 'UTC'
      });

      const queryAndParams = query.buildSqlAndParams();
      const sql = queryAndParams[0];

      // Should have date filtering
      expect(sql).toMatch(/WHERE/i);
      // Should have grouping by day
      expect(sql).toMatch(/GROUP BY/i);
    });

    it('should support multiple granularities on same local time dimension', async () => {
      await compiler.compile();

      const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: ['orders.count'],
        timeDimensions: [
          {
            dimension: 'orders.local_date',
            granularity: 'day',
            dateRange: ['2025-01-01', '2025-01-31']
          },
          {
            dimension: 'orders.local_hour',
            granularity: 'hour',
            dateRange: ['2025-01-01', '2025-01-31']
          }
        ],
        timezone: 'UTC'
      });

      const queryAndParams = query.buildSqlAndParams();
      const sql = queryAndParams[0];

      // Both dimensions should be in the query
      expect(sql).toContain('DATE(created_at AT TIME ZONE \'America/Los_Angeles\')');
      expect(sql).toContain('DATE_TRUNC(\'hour\', created_at AT TIME ZONE \'America/Los_Angeles\')');
    });
  });

  describe('Edge cases', () => {
    it('should handle local time dimensions without granularity', async () => {
      await compiler.compile();

      const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
        dimensions: ['orders.local_date'],
        measures: ['orders.count'],
        timezone: 'UTC'
      });

      const queryAndParams = query.buildSqlAndParams();
      const sql = queryAndParams[0];

      // Should include the dimension without timezone conversion
      expect(sql).toContain('DATE(created_at AT TIME ZONE \'America/Los_Angeles\')');
    });

    it('should handle local time dimensions in filters', async () => {
      await compiler.compile();

      const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: ['orders.count'],
        filters: [{
          member: 'orders.local_date',
          operator: 'inDateRange',
          values: ['2025-01-01', '2025-01-31']
        }],
        timezone: 'America/New_York'
      });

      const queryAndParams = query.buildSqlAndParams();
      const sql = queryAndParams[0];

      // Should filter on the local date dimension
      expect(sql).toMatch(/WHERE/i);
      expect(sql).toContain('DATE(created_at AT TIME ZONE \'America/Los_Angeles\')');
    });
  });

  describe('ISO 8601 Timezone Suffix Stripping', () => {
    it('strips Z (UTC) suffix', async () => {
      const { compiler, joinGraph, cubeEvaluator } = prepareJsCompiler(`
        cube(\`orders\`, {
          sql: \`SELECT * FROM orders\`,
          measures: {
            count: { type: 'count' }
          },
          dimensions: {
            createdAt: {
              sql: 'created_at',
              type: 'time',
              localTime: true
            }
          }
        })
      `);

      await compiler.compile();

      const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: ['orders.count'],
        timeDimensions: [{
          dimension: 'orders.createdAt',
          dateRange: ['2025-01-01T00:00:00.000Z', '2025-01-31T23:59:59.999Z']
        }],
        timezone: 'UTC'
      });

      const queryAndParams = query.buildSqlAndParams();
      const params = queryAndParams[1];
      
      // Parameters should not have Z suffix for local time
      expect(params[0]).not.toContain('Z');
      expect(params[1]).not.toContain('Z');
      expect(params[0]).toBe('2025-01-01T00:00:00.000');
      expect(params[1]).toBe('2025-01-31T23:59:59.999');
    });

    it('strips +hh:mm offset format', async () => {
      const { compiler, joinGraph, cubeEvaluator } = prepareJsCompiler(`
        cube(\`orders\`, {
          sql: \`SELECT * FROM orders\`,
          measures: {
            count: { type: 'count' }
          },
          dimensions: {
            createdAt: {
              sql: 'created_at',
              type: 'time',
              localTime: true
            }
          }
        })
      `);

      await compiler.compile();

      const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: ['orders.count'],
        timeDimensions: [{
          dimension: 'orders.createdAt',
          dateRange: ['2025-01-01T00:00:00.000+05:30', '2025-01-31T23:59:59.999+05:30']
        }],
        timezone: 'UTC'
      });

      const queryAndParams = query.buildSqlAndParams();
      const params = queryAndParams[1];
      
      // Parameters should not have +05:30 offset for local time
      expect(params[0]).not.toContain('+05:30');
      expect(params[1]).not.toContain('+05:30');
      expect(params[0]).toBe('2025-01-01T00:00:00.000');
      expect(params[1]).toBe('2025-01-31T23:59:59.999');
    });

    it('strips -hh:mm offset format', async () => {
      const { compiler, joinGraph, cubeEvaluator } = prepareJsCompiler(`
        cube(\`orders\`, {
          sql: \`SELECT * FROM orders\`,
          measures: {
            count: { type: 'count' }
          },
          dimensions: {
            createdAt: {
              sql: 'created_at',
              type: 'time',
              localTime: true
            }
          }
        })
      `);

      await compiler.compile();

      const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: ['orders.count'],
        timeDimensions: [{
          dimension: 'orders.createdAt',
          dateRange: ['2025-01-01T00:00:00.000-08:00', '2025-01-31T23:59:59.999-08:00']
        }],
        timezone: 'UTC'
      });

      const queryAndParams = query.buildSqlAndParams();
      const params = queryAndParams[1];
      
      // Parameters should not have -08:00 offset for local time
      expect(params[0]).not.toContain('-08:00');
      expect(params[1]).not.toContain('-08:00');
      expect(params[0]).toBe('2025-01-01T00:00:00.000');
      expect(params[1]).toBe('2025-01-31T23:59:59.999');
    });

    it('strips +hhmm compact offset format', async () => {
      const { compiler, joinGraph, cubeEvaluator } = prepareJsCompiler(`
        cube(\`orders\`, {
          sql: \`SELECT * FROM orders\`,
          measures: {
            count: { type: 'count' }
          },
          dimensions: {
            createdAt: {
              sql: 'created_at',
              type: 'time',
              localTime: true
            }
          }
        })
      `);

      await compiler.compile();

      const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: ['orders.count'],
        timeDimensions: [{
          dimension: 'orders.createdAt',
          dateRange: ['2025-01-01T00:00:00.000+0530', '2025-01-31T23:59:59.999+0530']
        }],
        timezone: 'UTC'
      });

      const queryAndParams = query.buildSqlAndParams();
      const params = queryAndParams[1];
      
      // Parameters should not have +0530 offset for local time
      expect(params[0]).not.toContain('+0530');
      expect(params[1]).not.toContain('+0530');
      expect(params[0]).toBe('2025-01-01T00:00:00.000');
      expect(params[1]).toBe('2025-01-31T23:59:59.999');
    });

    it('strips +hh hour-only offset format', async () => {
      const { compiler, joinGraph, cubeEvaluator } = prepareJsCompiler(`
        cube(\`orders\`, {
          sql: \`SELECT * FROM orders\`,
          measures: {
            count: { type: 'count' }
          },
          dimensions: {
            createdAt: {
              sql: 'created_at',
              type: 'time',
              localTime: true
            }
          }
        })
      `);

      await compiler.compile();

      const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: ['orders.count'],
        timeDimensions: [{
          dimension: 'orders.createdAt',
          dateRange: ['2025-01-01T00:00:00.000+05', '2025-01-31T23:59:59.999+05']
        }],
        timezone: 'UTC'
      });

      const queryAndParams = query.buildSqlAndParams();
      const params = queryAndParams[1];
      
      // Parameters should not have +05 offset for local time
      expect(params[0]).not.toContain('+05');
      expect(params[1]).not.toContain('+05');
      expect(params[0]).toBe('2025-01-01T00:00:00.000');
      expect(params[1]).toBe('2025-01-31T23:59:59.999');
    });

    it('handles dates without timezone suffixes unchanged', async () => {
      const { compiler, joinGraph, cubeEvaluator } = prepareJsCompiler(`
        cube(\`orders\`, {
          sql: \`SELECT * FROM orders\`,
          measures: {
            count: { type: 'count' }
          },
          dimensions: {
            createdAt: {
              sql: 'created_at',
              type: 'time',
              localTime: true
            }
          }
        })
      `);

      await compiler.compile();

      const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: ['orders.count'],
        timeDimensions: [{
          dimension: 'orders.createdAt',
          dateRange: ['2025-01-01T00:00:00.000', '2025-01-31T23:59:59.999']
        }],
        timezone: 'UTC'
      });

      const queryAndParams = query.buildSqlAndParams();
      const params = queryAndParams[1];
      
      // Parameters should remain unchanged
      expect(params[0]).toBe('2025-01-01T00:00:00.000');
      expect(params[1]).toBe('2025-01-31T23:59:59.999');
    });
  });
});
